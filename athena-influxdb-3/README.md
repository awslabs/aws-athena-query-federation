# Amazon Athena InfluxDB 3 Connector

This connector enables Amazon Athena to query **InfluxDB 3** time-series data (including **Amazon Timestream for InfluxDB**) using Apache Arrow Flight SQL. It runs as an AWS Lambda function that Athena invokes to list schemas/tables, describe tables, plan splits, and read records.

## Contents
- [Overview](#overview)
- [Supported and unsupported data types](#supported-and-unsupported-data-types)
- [Predicate & compute pushdown](#predicate--compute-pushdown)
- [Query passthrough](#query-passthrough)
- [Split strategy & query parallelism](#split-strategy--query-parallelism)
- [Record reads (field extraction)](#record-reads-field-extraction)
- [Backpressure](#backpressure)
- [Authentication & secrets](#authentication--secrets)
- [Deployment](#deployment)
- [Configuration parameters](#configuration-parameters)
- [Best practices](#best-practices)
- [Limitations](#limitations)
- [Running the integration tests](#running-the-integration-tests)

## Overview

InfluxDB 3 speaks SQL natively over Arrow Flight SQL, so this connector constructs standard SQL directly rather than translating to a bespoke query API. Metadata (databases, tables, columns) is read from `information_schema`; the control-plane list of databases is read from the InfluxDB `/api/v3/configure/database` HTTP endpoint.

- **Databases** map to Athena **schemas**.
- **Measurements** map to Athena **tables**.
- **Tags** and **fields** map to columns; every table has a `time` column.

Athena lowercases identifiers, but InfluxDB is case-sensitive; the connector resolves the original case (database via the configure API, table via `information_schema`) and stashes it in the table schema's custom metadata so reads use the correct names.

## Supported and unsupported data types

InfluxDB 3 (DataFusion/IOx) exposes a **flat, primitive** column model. The connector maps:

| InfluxDB / DataFusion type | Arrow / Athena type |
|---|---|
| `BIGINT`, `INTEGER` | `BIGINT` |
| `DOUBLE`, `FLOAT` | `FLOAT8` (double) |
| `BOOLEAN` | `BIT` (boolean) |
| `TIMESTAMP` (`time`) | `TIMESTAMP` (millisecond, UTC) |
| `VARCHAR`, tag (`Dictionary(Int32, Utf8)`) | `VARCHAR` |
| anything else | `VARCHAR` (safe fallback) |

**Unsupported (by design):** complex/nested types — `STRUCT`, `MAP`, `LIST`/`ARRAY` — and `DECIMAL`/`SET`. This is not a connector limitation so much as a property of the source: InfluxDB 3 measurements are flat rows of tags (strings), primitive fields, and a timestamp. There is no nested data to project, so no `STRUCT`/`MAP` handling is implemented. Timestamps are always normalized to millisecond precision in UTC regardless of the source unit (see [Record reads](#record-reads-field-extraction)).

## Predicate & compute pushdown

The connector advertises its capabilities via `doGetDataSourceCapabilities` and pushes the following down into the SQL sent to InfluxDB (see `InfluxDBQueryBuilder`):

- **Filter pushdown** — equality, `IN` lists, `IS NULL`/`IS NOT NULL`, and open/closed ranges (`>`, `>=`, `<`, `<=`, `BETWEEN`), derived from the constraint summary `ValueSet`s (`buildWhereClause` → `toPredicate`).
- **Complex expression pushdown** — `AND`/`OR`/`NOT`, comparison and arithmetic operators, `LIKE`, `IN`, `IS DISTINCT FROM`, `NULLIF`, etc., from `constraints.getExpression()` (`toExpression`). Unsupported functions are skipped (logged at `warn`), so the query still runs correctly with residual filtering done by Athena.
- **`ORDER BY` / Top-N pushdown** — `buildOrderByClause` emits `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]`.
- **`LIMIT` pushdown** — appended when `constraints.hasLimit()`.

**Why push down:** InfluxDB/DataFusion is far more efficient at filtering, sorting, and limiting at the source than shipping rows to Athena and filtering there. Pushdown minimizes the bytes marshalled back through the Lambda. Values are rendered as SQL literals with proper quoting/escaping; Athena's Trino-packed `TIMESTAMP WITH TIME ZONE` constants are unpacked to UTC millisecond `TIMESTAMP` literals so comparisons against the source `time` column are apples-to-apples.

## Query passthrough

The connector supports **query passthrough**, letting you run native InfluxDB 3 SQL verbatim (bypassing Athena's SQL planning) via the `system.query` table function:

```sql
SELECT * FROM TABLE(
  system.query(
    DATABASE => 'mydb',
    QUERY => 'SELECT host, mean(usage_idle) FROM cpu WHERE time > now() - INTERVAL ''1 hour'' GROUP BY host'
  )
);
```

- `DATABASE` — the InfluxDB database the query runs against (the Flight SQL client is per-database, so this is required).
- `QUERY` — the native InfluxDB 3 SQL to execute.

The result schema is inferred by running the query and inspecting the Arrow schema of its result, mapped to the same Athena types as normal reads (timestamps → millisecond UTC, tags/strings → VARCHAR, etc.). Passthrough is enabled by default; set `enable_query_passthrough=false` to disable it.

## Split strategy & query parallelism
A **split** is Athena's unit of parallelism — each split is read by a separate concurrent invocation of the RecordHandler Lambda.

- **Default (parallelism disabled):** the connector emits a single split that scans the whole (filtered) measurement. `enhancePartitionSchema` adds no partition columns, so the SDK short-circuits `doGetTableLayout` and `getPartitions` is not invoked.
- **Optional time-based splitting (parallelism enabled):** when `enable_query_parallelism=true` **and** the query carries a lower+upper bound on `time`, `getPartitions` divides that time range into `query_parallelism_count` half-open `[low, high)` buckets, and `doGetSplits` emits one split per bucket carrying its bounds. Each split's `readWithConstraint` injects `time >= low AND time < high` alongside the user's own predicates.

**Rationale / when to enable:** DataFusion already parallelizes *compute* for a single query, so splitting does **not** speed up server-side computation. What it parallelizes is the *connector-side* work that DataFusion cannot — pulling the result stream, decoding Arrow batches, and spilling to S3 — across N Lambdas. Benchmarking a ~500k-row scan showed a clean, monotonic improvement (≈1.95× at 4 splits, 2.38× at 8, 3.06× at 16). It provides **no benefit for small results or for aggregations that return few rows**, and it adds Lambda invocations and concurrent load on the (single) InfluxDB instance.

**Enable it** for large, time-ranged scans that return many rows (or that approach a single Lambda's memory/time limits). Leave it **off** (the default) otherwise. Tune `query_parallelism_count` (clamped to a safe range) to balance throughput against backend load.

## Backpressure

If InfluxDB signals throttling — a Flight `RESOURCE_EXHAUSTED`, or an HTTP `429` on the database-list call — the connector raises `FederationThrottleException` (`isThrottle` in `InfluxDBConnectionFactory`). Athena treats this as backpressure and reduces the concurrency it drives, which protects the single downstream InfluxDB instance from being overwhelmed by Lambda fan-out. Throttles are surfaced before any rows are written, so Athena can safely retry.

## Authentication & secrets

The InfluxDB token is provided via the `INFLUXDB3_AUTH_TOKEN` env var, which may be either a literal token or a Secrets Manager reference using the SDK's `${secret_name}` pattern. Secret values may be a plain string (the whole value is the token) or JSON (the token is read from a configurable key, default `token`, via `INFLUXDB3_AUTH_TOKEN_KEY`).

The resolved token and the per-database clients are cached for the Lambda container's lifetime. On an auth failure (Flight `UNAUTHENTICATED`/`UNAUTHORIZED` or HTTP `401`/`403`) the connector invalidates the cached token and clients, re-resolves the token (picking up a rotated secret), and retries — bounded by `token_refresh_max_retries` (default 3) so a genuinely invalid token fails fast instead of looping.

## Deployment

Deploy with the SAM template (`athena-influxdb.yaml`) via the Serverless Application Repository or `sam deploy`.

### Build and deploy (SAM CLI)

Prerequisites: JDK 25, Maven, the SAM CLI, and AWS credentials for the target account.

1. **Build the shaded Lambda jar** (the template's `CodeUri` points at it):

   ```bash
   cd athena-influxdb-3
   mvn clean package
   ```

2. **Create a Secrets Manager secret** holding the InfluxDB token (a plain-string secret; or JSON with a `token` key):

   ```bash
   aws secretsmanager create-secret \
     --name athena-influxdb-token \
     --secret-string '<your-influxdb-token>' \
     --region <region>
   ```

3. **Deploy.** For **Amazon Timestream for InfluxDB**, pass `VpcId` + `SubnetIds` so the template creates the connector's security group; the subnets must be able to reach Secrets Manager, S3, and Athena (see [Networking](#networking-amazon-timestream-for-influxdb)):

   ```bash
   sam deploy \
     --template-file athena-influxdb.yaml \
     --stack-name athena-influxdb-connector \
     --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
     --resolve-s3 \
     --region <region> \
     --parameter-overrides \
       AthenaCatalogName=athena_influxdb \
       SpillBucket=<your-spill-bucket> \
       InfluxDBHost=https://<cluster-endpoint>:8181 \
       InfluxDBSecretId=athena-influxdb-token \
       VpcId=<vpc-id> \
       "SubnetIds=<subnet-1>,<subnet-2>,<subnet-3>" \
       InfluxDBPort=8181
   ```

   For a non-VPC source (e.g., InfluxDB Cloud reachable over the internet), omit `VpcId`/`SubnetIds`.

4. **Add the cluster ingress rule (manual, one time).** After the stack reaches `CREATE_COMPLETE`, read its Outputs and apply the `RequiredClusterSecurityGroupRule` to your cluster's security group:

   ```bash
   aws cloudformation describe-stacks --stack-name athena-influxdb-connector \
     --region <region> --query 'Stacks[0].Outputs' --output table
   ```

   Then add an **inbound** rule to your **cluster's** security group — Custom TCP, port `8181`, source = the `ConnectorSecurityGroupId` from the Outputs (or the NAT gateway's Elastic IP `/32` if the cluster is publicly accessible and the Lambda egresses via NAT).

5. **Register the Athena data source** (if not created automatically), so you can query it:

   ```bash
   aws athena create-data-catalog --name athena_influxdb --type LAMBDA \
     --parameters function=arn:aws:lambda:<region>:<account>:function:athena_influxdb \
     --region <region>
   ```

6. **Verify:**

   ```sql
   SHOW DATABASES IN `athena_influxdb`;
   ```

### Networking (Amazon Timestream for InfluxDB)

Timestream for InfluxDB clusters live in a VPC, so the connector Lambda must be attached to that VPC via the `SubnetIds` and `SecurityGroupIds` parameters. Two things to get right:

1. **Reaching AWS services from the VPC.** A VPC-attached Lambda loses default internet egress. It still needs to reach **Secrets Manager**, **S3** (spill), and **Athena** (`GetQueryExecution`). Provide this with **VPC endpoints** (interface endpoints for Secrets Manager and Athena; a gateway endpoint for S3) **or** a **NAT gateway**. If you use **interface VPC endpoints** and let the template create the connector's security group, remember that those endpoints are themselves gated by a security group — add an inbound rule to the **endpoints' security group** allowing **TCP 443 from the connector's security group** (the `ConnectorSecurityGroupId` output), or the Lambda's calls to Secrets Manager/Athena will be blocked.
2. **Reaching the cluster.** Pass your `VpcId` (with `SubnetIds`) and the template **creates a dedicated security group** for the Lambda and attaches it. After the stack deploys, read the stack **Outputs** — `ConnectorSecurityGroupId` and `RequiredClusterSecurityGroupRule` tell you the exact inbound rule to add to your **cluster's** security group (Custom TCP, the InfluxDB port, source = the connector's security group). This one manual step is intentionally left to you so that the template never modifies a security group it doesn't own.
   - The scoped, security-group-referenced rule assumes the cluster is reached over a **private** address (cluster *not* publicly accessible), the recommended posture.
   - If the cluster is **publicly accessible**, its endpoint resolves to a **public IP** that a VPC Lambda cannot reach without a **NAT gateway**; in that case allow the **NAT's Elastic IP** (a `/32`) on the InfluxDB port instead — the Output text calls this out too.
   - If you prefer to manage the security group yourself, omit `VpcId` and pass your own `SecurityGroupIds`.

### Secret

Create a Secrets Manager secret holding the InfluxDB token and pass its name/ARN as `InfluxDBSecretId`. The Lambda's execution role (created by the template) is granted `secretsmanager:GetSecretValue` for that secret only.

## Configuration parameters

| Parameter / env var | Description |
|---|---|
| `AthenaCatalogName` | Lambda function name and Athena catalog name. |
| `SpillBucket` / `SpillPrefix` | S3 location for spilled results. |
| `INFLUXDB3_HOST_URL` (`InfluxDBHost`) | InfluxDB 3 host URL, e.g. `https://<endpoint>:8181`. |
| `INFLUXDB3_AUTH_TOKEN` (`InfluxDBSecretId`) | Token, or `${secret_name}` Secrets Manager reference. |
| `INFLUXDB3_AUTH_TOKEN_KEY` (`InfluxDBTokenKey`) | Key to read from a JSON secret (default `token`). |
| `influxdb_database` (`InfluxDBDatabase`) | Optional default database; if empty, all accessible databases are exposed. |
| `enable_query_parallelism` | `true` to enable time-based split parallelism (default `false`). |
| `query_parallelism_count` | Number of time buckets/splits when parallelism is enabled (default `8`, clamped). |
| `enable_query_passthrough` | `true` (default) to expose the `system.query` passthrough table function. |
| `token_refresh_max_retries` | Max token-refresh retries on auth failure (default `3`). |
| `SubnetIds` / `SecurityGroupIds` | VPC config (required for Timestream for InfluxDB). |
| `VpcId` | If set (with `SubnetIds`), the template **creates a dedicated security group** for the Lambda in this VPC and outputs the exact cluster ingress rule to add. If empty, `SecurityGroupIds` is used as-is. |
| `InfluxDBPort` | Cluster port (default `8181`); used to render the required ingress rule in the stack Outputs. |
| `LambdaTimeout` / `LambdaMemory` | Lambda runtime limits. |
| `DisableSpillEncryption` | Disable spill encryption (not recommended). |

## Best practices

- **Filter on `time`.** A bounded time range is the single most effective way to reduce scanned data, and it is what enables split parallelism.
- **Project only needed columns.** The connector reads exactly the projected columns; `SELECT *` over wide measurements marshals more data.
- **Keep the cluster private.** A non-public cluster lets you use a clean, security-group-scoped ingress rule and avoids a NAT gateway.
- **Enable parallelism deliberately.** Turn it on for large, time-ranged scans; leave it off for small/aggregate queries (see [Split strategy](#split-strategy--query-parallelism)).
- **Store the token in Secrets Manager** (via `${secret_name}`) rather than inline, and rely on the connector's rotation-aware token refresh.

## Limitations

- **Primitive types only** — no `STRUCT`/`MAP`/`LIST`/`DECIMAL` (see [data types](#supported-and-unsupported-data-types)).
- **Time-based splitting only.** Parallelism requires a lower+upper `time` bound; queries without one run as a single split.
- **Single-instance backpressure.** The backend is typically a single InfluxDB cluster; excessive parallelism can throttle it (surfaced to Athena as backpressure).
- **Cached token staleness window.** A rotated secret is picked up on the next auth failure or container recycle, not proactively.
- **Timestamps normalized to millisecond UTC.** Sub-millisecond precision from nanosecond sources is truncated on read.

## Running the integration tests

`InfluxDBLocalIntegrationTest` runs the connector against a local **InfluxDB 3 Core** container, so it exercises the real Flight SQL read/metadata paths end-to-end. It requires a working Docker daemon; no AWS resources or `test-config.json` are needed.

```bash
# From the athena-influxdb-3 directory:
mvn verify                 # runs unit + local integration tests
mvn test                   # unit tests only
mvn test jacoco:report     # unit tests + coverage report (target/site/jacoco)
```

To run against a real Timestream for InfluxDB cluster, deploy the connector (see [Deployment](#deployment)), register it as an Athena data source, and issue federated queries such as:

```sql
SHOW DATABASES IN `your_catalog`;
SELECT * FROM your_db.cpu
  WHERE time >= TIMESTAMP '2026-06-01 00:00:00 UTC'
    AND time <  TIMESTAMP '2026-06-07 00:00:00 UTC';
```

## License

This project is licensed under the Apache-2.0 License. Each source file carries the Apache 2.0 header, and the connector is packaged with `LICENSE.txt`.
