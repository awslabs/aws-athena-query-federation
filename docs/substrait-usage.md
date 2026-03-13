# Substrait Usage in Athena Query Federation

## Overview

This repository uses [Substrait](https://substrait.io/) as a language-agnostic, cross-platform representation of query plans. Athena sends a serialized Substrait plan alongside the existing `Constraints` object in federated query requests. Connectors can parse this plan to implement advanced predicate pushdown — including filters, projections, ordering, and limits — directly against their data sources.

The Substrait plan arrives as a **Base64-encoded Protocol Buffer** inside the `QueryPlan` object, which is part of the `Constraints` passed to both `MetadataHandler` and `RecordHandler`.

## Dependencies

Defined in the root `pom.xml` (version `0.65.0`):

| Artifact | Purpose |
|---|---|
| `io.substrait:core` | Protobuf definitions and expression builders (`ExpressionCreator`, `Plan`, `Rel`, etc.) |
| `io.substrait:isthmus` | Substrait ↔ Apache Calcite conversion (`SubstraitToCalcite`, `ProtoPlanConverter`) |

Apache Calcite `1.40.0` is used alongside Isthmus for SQL generation and relational algebra manipulation.

## Architecture

### Entry Point: `QueryPlan`

```
Athena Engine
  └─ Constraints
       └─ QueryPlan
            ├─ substraitVersion: String
            └─ substraitPlan: String (Base64-encoded protobuf)
```

`QueryPlan` (`athena-federation-sdk/.../domain/predicate/QueryPlan.java`) is the wire-format container. Connectors access it via:

```java
QueryPlan queryPlan = request.getConstraints().getQueryPlan();
String planString = queryPlan.getSubstraitPlan();
```

### SDK Substrait Package

All Substrait parsing logic lives in `athena-federation-sdk` under the package:

```
com.amazonaws.athena.connector.substrait
├── SubstraitRelUtils           # Deserialize plan, extract relation nodes
├── SubstraitFunctionParser     # Parse filter expressions into ColumnPredicates
├── SubstraitLiteralConverter   # Convert between Substrait literals and Arrow types
├── SubstraitMetadataParser     # Extract table column metadata from ReadRel
├── SubstraitSqlUtils           # Convert Substrait plan → SQL via Calcite
├── SubstraitAccumulatorVisitor # Parameterize SQL literals for prepared statements
├── SubstraitTypeAndValue       # Holds a typed literal value with column association
└── model/
    ├── SubstraitRelModel       # Structured wrapper for all Rel types in a plan
    ├── SubstraitOperator       # Enum: =, !=, >, <, >=, <=, IS NULL, AND, OR, NOT
    ├── ColumnPredicate         # A single column filter (column, operator, value, ArrowType)
    └── SubstraitField          # Schema field with name, type, and nested children
```

## Key Classes

### `SubstraitRelUtils`

Deserializes the Base64 plan and traverses the relation tree to extract specific node types.

```java
// Deserialize
Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(base64String);

// Extract individual relations from the tree
Rel rel = plan.getRelations(0).getRoot().getInput();
ReadRel   readRel   = SubstraitRelUtils.getReadRel(rel);    // Table scan
FilterRel filterRel = SubstraitRelUtils.getFilterRel(rel);   // WHERE clause
SortRel   sortRel   = SubstraitRelUtils.getSortRel(rel);     // ORDER BY
FetchRel  fetchRel  = SubstraitRelUtils.getFetchRel(rel);    // LIMIT
ProjectRel projectRel = SubstraitRelUtils.getProjectRel(rel); // SELECT columns
```

### `SubstraitRelModel`

Convenience wrapper that extracts all relation types from a plan in one call:

```java
SubstraitRelModel model = SubstraitRelModel.buildSubstraitRelModel(rel);
model.getReadRel();     // table scan info + base schema
model.getFilterRel();   // filter conditions
model.getFetchRel();    // limit/offset
model.getSortRel();     // ordering
model.getProjectRel();  // column projection
```

### `SubstraitFunctionParser`

Parses the filter expression from a `FilterRel` into a list of `ColumnPredicate` objects. Recursively flattens AND/OR logical operators.

```java
FilterRel filterRel = model.getFilterRel();
List<String> columns = SubstraitMetadataParser.getTableColumns(model);

Map<String, List<ColumnPredicate>> predicatesByColumn =
    SubstraitFunctionParser.getColumnPredicatesMap(
        plan.getExtensionsList(),
        filterRel.getCondition(),
        columns);
```

Supported operators: `gt`, `gte`, `lt`, `lte`, `equal`, `not_equal`, `is_null`, `is_not_null`, `and`, `or`.

### `SubstraitLiteralConverter`

Bidirectional conversion between Substrait protobuf literals and Java/Arrow types.

- `extractLiteralValue(Expression)` → `Pair<Object, ArrowType>` — reads a literal from the plan
- `createLiteralExpression(Object, ArrowType)` → `io.substrait.expression.Expression` — creates a literal for plan construction

Supported types: i8, i16, i32, i64, fp32, fp64, string, boolean, binary, decimal, date, timestamp, varchar, fixed_char.

**Unsupported Substrait Literal Types** (`extractLiteralValue` — throws `UnsupportedOperationException`):

| Category | Types |
|---|---|
| Numeric | `UUID`, `INTERVAL_YEAR_TO_MONTH`, `INTERVAL_DAY_TO_SECOND` |
| Temporal | `TIME`, `PRECISION_TIMESTAMP`, `PRECISION_TIMESTAMP_TZ` |
| Complex/Structured | `LIST`, `MAP`, `STRUCT`, `EMPTY_LIST`, `EMPTY_MAP`, `NULL` |
| Other | `FIXED_BINARY`, `USER_DEFINED` |

**Unsupported Arrow Type IDs** (`createLiteralExpression` — throws `IllegalArgumentException`):

| Category | Types |
|---|---|
| Temporal | `Time`, `Duration`, `Interval` |
| Complex/Structured | `List`, `Map`, `Struct`, `Union`, `LargeList`, `ListView` |
| Other | `FixedSizeBinary`, `LargeBinary`, `LargeUtf8`, `Null`, `NONE` |

### `SubstraitMetadataParser`

Extracts table column names from the `ReadRel`'s base schema, including support for nested struct types.

```java
List<String> columns = SubstraitMetadataParser.getTableColumns(model);
```

### `SubstraitSqlUtils`

Converts a Substrait plan into a SQL AST (`SqlNode`) using the Isthmus library and Apache Calcite. Used by JDBC connectors to generate native SQL.

```java
// Get SQL AST for a specific dialect
SqlNode sqlNode = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(planString, sqlDialect);

// Get the base table schema
RelDataType schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(planString, sqlDialect);

// Get column rename mapping (Calcite-renamed → original)
Map<String, String> remapping = SubstraitSqlUtils.getColumnRemapping(planString, sqlDialect);
```

### `SubstraitAccumulatorVisitor`

A Calcite `SqlShuttle` that walks a SQL AST and replaces literal values with `SqlDynamicParam` placeholders, collecting the extracted values into an accumulator list. This enables JDBC connectors to use prepared statements with parameterized queries.

Handles: binary comparisons, IN, BETWEEN, LIKE, IS NULL/IS NOT NULL, boolean columns, and logical operators (AND/OR/NOT).

## Connector Integration Patterns

### Pattern 1: JDBC Connectors (SQL Generation)

The `athena-jdbc` module converts the Substrait plan into a native SQL query with parameterized values.

**Flow:**
1. Check if `constraints.getQueryPlan()` is non-null
2. Convert plan → `SqlNode` via `SubstraitSqlUtils`
3. Use `SubstraitAccumulatorVisitor` to parameterize literals
4. Execute as a JDBC `PreparedStatement` with bound parameters
5. Use `SubstraitSqlUtils.getColumnRemapping()` to map renamed columns back to originals

**Key files:**
- `JdbcSplitQueryBuilder` — builds the SQL query from the Substrait plan
- `JdbcRecordHandler` — handles column remapping during result reading

### Pattern 2: NoSQL Connectors (Predicate Extraction)

The `athena-dynamodb` module extracts individual column predicates for DynamoDB query/scan operations.

**Flow:**
1. Deserialize the plan via `SubstraitRelUtils`
2. Build a `SubstraitRelModel` from the relation tree
3. Extract column predicates via `SubstraitFunctionParser`
4. Use predicates to construct DynamoDB `QueryRequest` or `ScanRequest` with key conditions and filter expressions
5. Extract `FetchRel` for LIMIT pushdown

**Key files:**
- `DynamoDBMetadataHandler` — extracts predicates during split planning
- `DynamoDBRecordHandler` — applies LIMIT from `FetchRel`
- `DDBPredicateUtils` — converts `ColumnPredicate` objects into DynamoDB expressions

## Adding Substrait Support to a New Connector

1. Check for the plan:
   ```java
   if (constraints.getQueryPlan() != null
       && constraints.getQueryPlan().getSubstraitPlan() != null) {
       // Use Substrait-based pushdown
   }
   ```

2. For SQL-based connectors, use `SubstraitSqlUtils` to generate native SQL.

3. For non-SQL connectors, use `SubstraitFunctionParser` to extract `ColumnPredicate` objects and translate them into your data source's native filter format.

4. For LIMIT pushdown, extract `FetchRel`:
   ```java
   Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
   Rel rel = plan.getRelations(0).getRoot().getInput();
   FetchRel fetchRel = SubstraitRelUtils.getFetchRel(rel);
   if (fetchRel != null) {
       long limit = fetchRel.getCount();
   }
   ```

## Serialization

`QueryPlan` is serialized/deserialized via `QueryPlanSerDe` (in the v6 serde package), which is registered through `ConstraintsSerDeV6` and `ObjectMapperFactoryV6`. The plan travels as a JSON field within the `Constraints` object.
