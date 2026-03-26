# Integrating Athena Federation Connectors with Non-Athena Engines via Substrait

## Overview

The Athena Federation SDK connectors communicate via a JSON-over-Lambda protocol. Any query engine can invoke these connectors by constructing the correct JSON request payloads and invoking the connector's Lambda function. The Substrait query plan is the key mechanism for pushing down rich query semantics (filters, projections, ordering, limits) from the engine to the connector.

This document describes how a non-Athena engine can produce and send Substrait plans to existing Athena Federation connectors, and how to build an adapter layer that bridges the two.

## Architecture

```
┌──────────────────┐         Lambda Invoke (JSON)         ┌──────────────────────┐
│                  │ ──────────────────────────────────▶   │                      │
│  Your Query      │   PingRequest                        │  Athena Federation   │
│  Engine          │   ListSchemasRequest                 │  Connector Lambda    │
│                  │   ListTablesRequest                  │                      │
│  (Presto, Trino, │   GetTableRequest                   │  (CompositeHandler)  │
│   Spark, custom) │   GetSplitsRequest  ← has Constraints│  ├─ MetadataHandler  │
│                  │   ReadRecordsRequest ← has Constraints│  └─ RecordHandler    │
│                  │ ◀──────────────────────────────────   │                      │
│                  │   JSON responses + Arrow blocks       │                      │
└──────────────────┘                                      └──────────────────────┘
```

The `Constraints` object in `GetSplitsRequest` and `ReadRecordsRequest` carries the `QueryPlan`, which contains the Base64-encoded Substrait protobuf plan.

## Protocol: Request/Response Lifecycle

The connector Lambda is a `RequestStreamHandler` that accepts JSON and dispatches based on request type. The full query lifecycle is:

### 1. Ping (capability discovery)

```json
{
  "@type": "PingRequest",
  "identity": { "id": "engine-id", "principal": "engine-principal", "account": "123456789012" },
  "catalogName": "my_catalog",
  "queryId": "query-001"
}
```

Response includes `serDeVersion` (currently `6`) and `capabilities`. Your engine should use the returned `serDeVersion` to determine which serialization format to use for subsequent requests.

### 2. ListSchemas → ListTables → GetTable (metadata discovery)

Standard metadata calls to discover schemas, tables, and column definitions. These do not involve Substrait.

### 3. GetSplits (split planning with Substrait)

This is where the engine provides the Substrait plan. The connector uses it to determine how to partition the read.

```json
{
  "@type": "GetSplitsRequest",
  "identity": { ... },
  "queryId": "query-001",
  "catalogName": "my_catalog",
  "tableName": { "schemaName": "my_schema", "tableName": "my_table" },
  "partitions": { ... },
  "partitionCols": [],
  "constraints": {
    "summary": {},
    "expression": [],
    "orderByClause": [],
    "limit": -1,
    "queryPassthroughArguments": null,
    "queryPlan": {
      "substraitVersion": "0.65.0",
      "substraitPlan": "<Base64-encoded io.substrait.proto.Plan>"
    }
  }
}
```

### 4. ReadRecords (data retrieval with Substrait)

For each split returned, the engine sends a `ReadRecordsRequest` with the same `Constraints` (and therefore the same `QueryPlan`).

## Producing a Substrait Plan

### Option A: From SQL (using Isthmus)

The SDK's test utility `EncodedSubstraitPlanStringGenerator` demonstrates the canonical approach:

```java
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.PlanProtoConverter;
import io.substrait.proto.Plan;

// 1. Define the table schema as a Calcite Table
Table table = new AbstractTable() {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
        return factory.createStructType(Arrays.asList(
            Pair.of("id", factory.createSqlType(SqlTypeName.INTEGER)),
            Pair.of("name", factory.createSqlType(SqlTypeName.VARCHAR)),
            Pair.of("price", factory.createSqlType(SqlTypeName.DECIMAL, 10, 2))
        ));
    }
};

// 2. Build a Calcite catalog with the schema
CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
rootSchema.add("my_schema", new SubstraitSchema(Map.of("my_table", table)));
CalciteCatalogReader catalog = new CalciteCatalogReader(
    rootSchema,
    List.of("my_schema"),
    new SqlTypeFactoryImpl(AnsiSqlDialect.DEFAULT.getTypeSystem()),
    CalciteConnectionConfig.DEFAULT
        .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString())
);

// 3. Convert SQL → Substrait Plan
SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
PlanProtoConverter planProtoConverter = new PlanProtoConverter();
String sql = "SELECT id, name FROM my_table WHERE price > 10.00 ORDER BY name LIMIT 100";
Plan plan = planProtoConverter.toProto(sqlToSubstrait.convert(sql, catalog));

// 4. Base64-encode for the QueryPlan payload
String encodedPlan = Base64.getEncoder().encodeToString(plan.toByteArray());
```

### Option B: Programmatic Plan Construction (using Substrait protobuf API)

For engines that work with relational algebra directly rather than SQL:

```java
import io.substrait.proto.*;

// Build a ReadRel (table scan) with schema
ReadRel.Builder readRel = ReadRel.newBuilder()
    .setBaseSchema(NamedStruct.newBuilder()
        .addNames("id").addNames("name").addNames("price")
        .setStruct(Type.Struct.newBuilder()
            .addTypes(Type.newBuilder().setI32(Type.I32.newBuilder()))
            .addTypes(Type.newBuilder().setVarchar(Type.VarChar.newBuilder().setLength(256)))
            .addTypes(Type.newBuilder().setDecimal(Type.Decimal.newBuilder()
                .setPrecision(10).setScale(2)))));

// Build a FilterRel (WHERE price > 10.00)
// ... construct Expression with ScalarFunction referencing "gt:any_any" ...

// Build a FetchRel (LIMIT 100)
FetchRel fetchRel = FetchRel.newBuilder()
    .setInput(filterRel)  // chain from filter
    .setCount(100)
    .build();

// Wrap in a Plan
Plan plan = Plan.newBuilder()
    .addRelations(PlanRel.newBuilder()
        .setRoot(RelRoot.newBuilder()
            .setInput(Rel.newBuilder().setFetch(fetchRel))))
    .build();

String encodedPlan = Base64.getEncoder().encodeToString(plan.toByteArray());
```

## What the Connector Does with the Plan

Different connector types consume the plan differently. Understanding this helps you produce plans that maximize pushdown.

### JDBC Connectors (MySQL, PostgreSQL, Redshift, etc.)

The JDBC connector path in `JdbcSplitQueryBuilder`:

1. Checks `constraints.getQueryPlan() != null`
2. Converts the Substrait plan → Calcite `RelNode` → `SqlNode` via `SubstraitSqlUtils.getSqlNodeFromSubstraitPlan()`
3. Uses `SubstraitAccumulatorVisitor` to parameterize all literals into `?` placeholders
4. Executes as a JDBC `PreparedStatement` with bound parameters
5. Uses `SubstraitSqlUtils.getColumnRemapping()` to map Calcite-renamed columns back to originals

This means JDBC connectors can push down the full SQL semantics: WHERE, ORDER BY, LIMIT, projections, and column expressions.

### NoSQL Connectors (DynamoDB, etc.)

The DynamoDB connector path:

1. Deserializes the plan: `SubstraitRelUtils.deserializeSubstraitPlan(planString)`
2. Builds a `SubstraitRelModel` to access individual relation nodes
3. Extracts column predicates via `SubstraitFunctionParser.getColumnPredicatesMap()`
4. Translates `ColumnPredicate` objects into DynamoDB key conditions and filter expressions
5. Extracts `FetchRel` for LIMIT pushdown

This means NoSQL connectors extract individual filter predicates rather than full SQL.

## Substrait Plan Structure Expected by Connectors

The connectors expect a plan with this general relation tree structure:

```
Plan
└── PlanRel
    └── RelRoot
        └── Rel (one of the following trees)
            ├── FetchRel (LIMIT/OFFSET)
            │   └── SortRel (ORDER BY)
            │       └── ProjectRel (SELECT columns)
            │           └── FilterRel (WHERE)
            │               └── ReadRel (table scan + base schema)
            │
            ├── FilterRel → ReadRel  (filter only, no ordering/limit)
            │
            └── ReadRel  (full table scan, no pushdown)
```

Not all nodes need to be present. The SDK traverses the tree recursively, so any subset works:
- `ReadRel` alone = full scan
- `FilterRel` → `ReadRel` = filtered scan
- `FetchRel` → `FilterRel` → `ReadRel` = filtered scan with limit
- `ProjectRel` → `FilterRel` → `ReadRel` = projected filtered scan

### Supported Filter Operators

The `SubstraitFunctionParser` maps these Substrait function names to operators:

| Substrait Function | Operator |
|---|---|
| `gt:any_any` | `>` |
| `gte:any_any` | `>=` |
| `lt:any_any` | `<` |
| `lte:any_any` | `<=` |
| `equal:any_any` | `=` |
| `not_equal:any_any` | `!=` |
| `is_null:any` | `IS NULL` |
| `is_not_null:any` | `IS NOT NULL` |
| `and:bool` | `AND` (logical, flattened recursively) |
| `or:bool` | `OR` (logical, flattened recursively) |

Functions must be declared in the plan's `extensions` list and referenced by anchor ID in `ScalarFunction` expressions.

### Supported Literal Types

| Substrait Literal | Arrow Type |
|---|---|
| `i8`, `i16`, `i32`, `i64` | `Int(8/16/32/64, signed)` |
| `fp32`, `fp64` | `FloatingPoint(SINGLE/DOUBLE)` |
| `string`, `var_char`, `fixed_char` | `Utf8` |
| `boolean` | `Bool` |
| `binary` | `Binary` |
| `decimal` | `Decimal(precision, scale)` |
| `date` | `Date(DAY)` |
| `timestamp`, `timestamp_tz` | `Timestamp(MICROSECOND)` |

## Building an Engine Adapter

### Minimal Adapter Components

```
┌─────────────────────────────────────────────────────┐
│                  Engine Adapter                      │
│                                                      │
│  1. CatalogProvider                                  │
│     - Calls PingRequest, ListSchemas, ListTables,    │
│       GetTable to populate engine's catalog           │
│                                                      │
│  2. SubstraitPlanBuilder                             │
│     - Converts engine's query plan / SQL fragment    │
│       into a Base64-encoded Substrait Plan           │
│                                                      │
│  3. SplitPlanner                                     │
│     - Sends GetSplitsRequest with QueryPlan          │
│     - Collects Split objects for parallel reads       │
│                                                      │
│  4. RecordReader                                     │
│     - Sends ReadRecordsRequest per split             │
│     - Deserializes Arrow blocks from response         │
│     - Handles spill reads from S3 if needed          │
│                                                      │
│  5. SchemaMapper                                     │
│     - Maps Arrow Schema ↔ engine's type system       │
│     - Handles column remapping from Substrait plan    │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Key Implementation Considerations

**SerDe Version Negotiation**: Always send a `PingRequest` first. The response's `serDeVersion` tells you which JSON serialization format to use. The current version is `6`. The `CompositeHandler` tries to deserialize starting from the latest version and falls back to older versions.

**Constraints Dual-Path**: The `Constraints` object carries both the legacy predicate representation (`summary` map of `ValueSet`, `expression` list, `orderByClause`, `limit`) and the new `queryPlan` field. Connectors check for `queryPlan` first and fall back to the legacy fields. Your engine should populate both for maximum compatibility:

```json
{
  "constraints": {
    "summary": { /* legacy ValueSet predicates */ },
    "expression": [ /* legacy FederationExpression list */ ],
    "orderByClause": [ /* legacy OrderByField list */ ],
    "limit": 100,
    "queryPlan": {
      "substraitVersion": "0.65.0",
      "substraitPlan": "<base64>"
    }
  }
}
```

**Arrow Data Format**: Responses use Apache Arrow as the wire format for data blocks. Your engine needs an Arrow reader to consume `ReadRecordsResponse` data. The schema uses Arrow's `Schema` type.

**Spill Handling**: For large result sets, the connector spills data to S3 rather than returning it inline. The response will contain `SpillLocation` references instead of inline blocks. Your adapter must read these from S3 and decrypt them using the `EncryptionKey` provided in the response.

**Column Remapping**: When using Substrait plans, Calcite may rename columns during plan conversion (e.g., `varchar_col` → `varchar_col0`). JDBC connectors use `SubstraitSqlUtils.getColumnRemapping()` to map these back. Your engine should be aware that the column names in the Arrow response may differ from the original schema names.

**Identity**: All requests require a `FederatedIdentity` object. For non-Athena engines, construct this with your engine's identity context:

```json
{
  "identity": {
    "id": "engine-user-id",
    "principal": "engine-principal",
    "account": "aws-account-id",
    "arn": "arn:aws:iam::123456789012:role/EngineRole"
  }
}
```

## Example: End-to-End Flow

```
Engine                                          Connector Lambda
  │                                                    │
  │─── PingRequest ──────────────────────────────────▶│
  │◀── PingResponse (serDeVersion=6) ─────────────────│
  │                                                    │
  │─── ListSchemasRequest ───────────────────────────▶│
  │◀── ListSchemasResponse ["schema1", "schema2"] ────│
  │                                                    │
  │─── GetTableRequest (schema1.orders) ─────────────▶│
  │◀── GetTableResponse (Arrow Schema) ───────────────│
  │                                                    │
  │  [Engine builds Substrait plan from its query]     │
  │  SQL: SELECT id, total FROM orders                 │
  │       WHERE status = 'active' LIMIT 50             │
  │  → Substrait: FetchRel(50) → FilterRel → ReadRel  │
  │  → Base64 encode                                   │
  │                                                    │
  │─── GetSplitsRequest (with QueryPlan) ────────────▶│
  │◀── GetSplitsResponse [split1, split2, split3] ────│
  │                                                    │
  │─── ReadRecordsRequest (split1 + QueryPlan) ──────▶│  ← parallel
  │─── ReadRecordsRequest (split2 + QueryPlan) ──────▶│  ← parallel
  │─── ReadRecordsRequest (split3 + QueryPlan) ──────▶│  ← parallel
  │◀── ReadRecordsResponse (Arrow blocks) ────────────│
  │◀── ReadRecordsResponse (Arrow blocks) ────────────│
  │◀── ReadRecordsResponse (Arrow blocks) ────────────│
  │                                                    │
  │  [Engine merges Arrow blocks into result set]      │
```

## Dependencies for Your Engine Adapter

```xml
<!-- Substrait plan construction -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>core</artifactId>
    <version>0.65.0</version>
</dependency>

<!-- SQL → Substrait conversion (if your engine works with SQL) -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>isthmus</artifactId>
    <version>0.65.0</version>
</dependency>

<!-- Arrow for reading response data -->
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>13.0.0</version>
</dependency>

<!-- Jackson for JSON serialization of requests/responses -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.18.2</version>
</dependency>
```

## Limitations

- The `SubstraitFunctionParser` only supports the operators listed above. Complex expressions (CASE, CAST, arithmetic, string functions) in filter predicates are not parsed by the NoSQL path — they are handled by the SQL path via Isthmus/Calcite conversion.
- The Substrait plan must use the standard Substrait function extension URIs so that Isthmus can resolve them during Calcite conversion.
- Column names in the `ReadRel.baseSchema` must match the table's actual column names as returned by `GetTableResponse`.
- The `queryPlan` field is only present in SerDe version 6 (`ConstraintsSerDeV6`). Older connector versions will ignore it.
