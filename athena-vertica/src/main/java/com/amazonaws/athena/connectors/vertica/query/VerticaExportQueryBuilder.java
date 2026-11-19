/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.vertica.query;

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitAccumulatorVisitor;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connector.substrait.SubstraitTypeAndValue;
import com.google.common.base.Joiner;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds the Vertica {@code EXPORT TO PARQUET} SQL, including Substrait-plan-driven column and type handling.
 */
public class VerticaExportQueryBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerticaExportQueryBuilder.class);
    private static final String TEMPLATE_NAME = "templateVerticaExportQuery";
    private static final String QPT_TEMPLATE_NAME = "templateVerticaExportQPTQuery";
    private static final String SUBSTRAIT_TEMPLATE_NAME = "templateVerticaExportSubstraitQuery";
    private static final String TEMPLATE_FIELD = "builder";
    private static final String QUOTE_CHARS = "\"";
    private static final DateTimeFormatter VERTICA_TIMESTAMP_LITERAL_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .toFormatter();
    private final ST query;
    private String s3ExportBucket;
    private String table;
    private String queryID;
    private String colNames;
    private String constraintValues;
    private String preparedStatementSQL;
    private String queryFromPlan;

    public VerticaExportQueryBuilder(ST template)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    static String getQptTemplateName()
    {
        return QPT_TEMPLATE_NAME;
    }

    static String getSubstraitTemplateName()
    {
        return SUBSTRAIT_TEMPLATE_NAME;
    }

    public String getQueryFromPlan()
    {
        return queryFromPlan;
    }

    public String getTable(){return table;}

    public VerticaExportQueryBuilder fromTable(String schemaName, String tableName)
    {
        this.table = PredicateBuilder.getFromClauseWithSplit(schemaName, tableName);
        return this;
    }

    public String getColNames() {return colNames;}

    public VerticaExportQueryBuilder withPreparedStatementSQL(String preparedStatementSQL)
    {
        this.preparedStatementSQL = preparedStatementSQL;
        return this;
    }

    public String getPreparedStatementSQL()
    {
        return preparedStatementSQL;
    }

    // get the column names from user issued query in Athena
    public VerticaExportQueryBuilder withColumns(ResultSet definition, Schema tableSchema) throws SQLException {
        //get column name and type from the Schema in a hashmap for future use
        HashMap<String, String> mapOfNamesAndTypes = new HashMap<>();

        while(definition.next())
        {
            String colName = definition.getString("COLUMN_NAME").toLowerCase();
            String colType = definition.getString("TYPE_NAME").toLowerCase();
            mapOfNamesAndTypes.put(colName, colType);
        }

        // get the column names from the table schema
        StringBuilder colN = new StringBuilder();
        List<Field> fields = tableSchema.getFields();
        for(Field f : fields)
        {
            /*
            Vertica exports timestamp/timestamptz field as a INT 96 (26 digit number). The solution implemented here adds a 'cast as varchar' statement
            to the timestamp column to export the field as a VARCHAR.
             */
            String col_type = mapOfNamesAndTypes.get(f.getName().toLowerCase());
            if(col_type.equals("timestamp") || col_type.equals("timestamptz"))
            {
                String castedField = castTimestamp(f.getName());
                colN.append(castedField).append(",");
            } else {
                colN.append(PredicateBuilder.quote(f.getName())).append(",");
            }
        }
        this.colNames = colN.deleteCharAt(colN.length() - 1).toString();
        return this;
    }

    public String getConstraintValues() {
        return constraintValues;}

    //get the constraints from user issued query in Athena
    public VerticaExportQueryBuilder withConstraints(Constraints constraints, Schema tableSchema)
    {

        StringBuilder stringBuilder = new StringBuilder();
        //Get the constraints
        HashMap<String, PredicateBuilder.TypeAndValue> accumulator = new HashMap<>();
        List<String> clauses =  PredicateBuilder.toConjuncts(tableSchema.getFields(), constraints, accumulator);

        // if clauses is not empty, add it to the templates
        if (!clauses.isEmpty())
        {
            stringBuilder.append("WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        ST sqlTemplate = new ST(stringBuilder.toString());

        for (Map.Entry<String, PredicateBuilder.TypeAndValue> entry : accumulator.entrySet())
        {
            PredicateBuilder.TypeAndValue typeAndValue = entry.getValue();
            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());
            String colName = entry.getKey();

            switch (minorTypeForArrowType)
            {
                case BIT:
                    int value = ((boolean) typeAndValue.getValue()) ? 1 : 0;
                    sqlTemplate.add(colName, value);
                    break;
                case TINYINT:
                    sqlTemplate.add(colName, Byte.parseByte(typeAndValue.getValue().toString()));
                    break;
                case SMALLINT:
                    sqlTemplate.add(colName,Short.parseShort(typeAndValue.getValue().toString()));
                    break;
                case INT:
                    sqlTemplate.add(colName, Integer.parseInt(typeAndValue.getValue().toString()));
                    break;
                case BIGINT:
                    sqlTemplate.add(colName,Long.parseLong(typeAndValue.getValue().toString()));
                    break;
                case FLOAT4:
                    sqlTemplate.add(colName,Float.parseFloat(typeAndValue.getValue().toString()));
                    break;
                case FLOAT8:
                    sqlTemplate.add(colName,Double.parseDouble(typeAndValue.getValue().toString()));
                    break;
                case DECIMAL:
                    sqlTemplate.add(colName, new BigDecimal(typeAndValue.getValue().toString()));
                    break;
                case DATEDAY:
                    sqlTemplate.add(colName, (int) LocalDate.parse(typeAndValue.getValue().toString()).toEpochDay());
                    break;
                case DATEMILLI:
                    sqlTemplate.add(colName, LocalDateTime.parse(typeAndValue.getValue().toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                    break;
                case VARCHAR:
                    String val = "'" + escapeSqlStringLiteral(typeAndValue.getValue().toString()) + "'";
                    sqlTemplate.add(colName, val);
                    break;
                case VARBINARY:
                    sqlTemplate.add(colName, typeAndValue.toString().getBytes());
                    break;

                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType));
            }
        }
        this.constraintValues =  sqlTemplate.render();
        return this;

    }

    protected String castTimestamp(String name)
    {
        String quotedColumnName = PredicateBuilder.quote(name);
        return "CAST(" + quotedColumnName + " AS VARCHAR) AS " + quotedColumnName;
    }

    private static String escapeSqlStringLiteral(String value)
    {
        return value.replace("'", "''");
    }

    /**
     * Renders a byte array as a Vertica {@code X'<hex>'} VARBINARY literal.
     * Required because StringTemplate has no default renderer for {@code byte[]}.
     */
    private static String bytesToVerticaHexLiteral(byte[] value)
    {
        StringBuilder hex = new StringBuilder(value.length * 2);
        for (byte b : value) {
            hex.append(String.format("%02X", b));
        }
        return "X'" + hex + "'";
    }

    //build the Vertica SQL to set the AWS Region
    public String buildSetAwsRegionSql(String awsRegion)
    {
        if (awsRegion == null || awsRegion.equals("")) { 
            awsRegion = "us-east-1"; 
        }
        return "ALTER SESSION SET AWSRegion='" + escapeSqlStringLiteral(awsRegion) + "'";
    }

    public String getS3ExportBucket(){return s3ExportBucket;}

    public VerticaExportQueryBuilder withS3ExportBucket(String s3ExportBucket)
    {
        this.s3ExportBucket = s3ExportBucket;
        return this;
    }

    public String getQueryID(){return queryID;}

    public VerticaExportQueryBuilder withQueryID(String queryID)
    {
        this.queryID = queryID;
        return this;
    }

    public String build()
    {
        Validate.notNull(s3ExportBucket, "s3ExportBucket can not be null.");
        Validate.notNull(table != null ? table : (preparedStatementSQL != null ? preparedStatementSQL : queryFromPlan), "table can not be null.");
        Validate.notNull(queryID, "queryID can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    /**
     * Converts a Substrait query plan into a Vertica-compatible SQL string by decoding,
     * cleaning aliases, casting timestamps, parameterizing WHERE literals, and converting
     * FETCH NEXT to LIMIT syntax.
     *
     * @param queryPlan The substrait query plan from Athena engine
     * @param sqlDialect The Vertica SQL dialect for Calcite rendering
     * @return this builder with queryFromPlan populated
     */
    public VerticaExportQueryBuilder withQueryPlan(final QueryPlan queryPlan, final SqlDialect sqlDialect) {
        String base64EncodedPlan = queryPlan.getSubstraitPlan();

        SqlNode sqlNode = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(base64EncodedPlan, sqlDialect);
        RelDataType tableSchema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(base64EncodedPlan, sqlDialect);

        if (!(sqlNode instanceof SqlSelect)) {
            throw new AthenaConnectorException("Unsupported Query Type. Only SELECT Query is supported.",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
        SqlSelect select = (SqlSelect) sqlNode;

        // Remove auto-generated columnName aliases and cast timestamp/timestamptz type fields
        select = processSelectList(select, tableSchema);

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        SubstraitAccumulatorVisitor visitor = new SubstraitAccumulatorVisitor(accumulator, tableSchema);
        select = (SqlSelect) select.accept(visitor);

        select = createParameterizedQuery(select);

        // Converts SQL Standard FETCH syntax to Vertica LIMIT syntax
        String sqlString = convertFetchToVerticaLimit(select);

        // Use '{' '}' delimiters to avoid conflicts with SQL's < > operators
        ST sqlTemplate = new ST(sqlString, '{', '}');
        handleDataTypesForSqlTemplate(accumulator, sqlTemplate);
        String renderedSql = sqlTemplate.render();
        // Defensive: SqlNode.toString() renders via AnsiSqlDialect which uses double-quote
        // identifiers (already Vertica-compatible), so this backtick replace is a no-op safety net.
        this.queryFromPlan = renderedSql.replace("`", "\"");
        return this;
    }

    /**
     * Cleans the SELECT list: removes auto-generated aliases and casts timestamp fields to VARCHAR.
     */
    private SqlSelect processSelectList(final SqlSelect select, final RelDataType tableSchema) {

        SqlNodeList selectList = expandSelectList(select, tableSchema);
        SqlNodeList newSelectList = new SqlNodeList(SqlParserPos.ZERO);

        for (SqlNode selectItem : selectList) {
            if (selectItem instanceof SqlIdentifier) {
                // Case 1: Simple column reference without alias - SELECT name FROM users
                SqlTypeName fieldType = resolveFieldType(selectItem.toString(), tableSchema);
                SqlNode processedItem = isTimestampType(fieldType) ? processTimestampColumnWithCasting(selectItem) : selectItem;
                newSelectList.add(processedItem);
            }
            else if (selectItem instanceof SqlBasicCall && selectItem.getKind() == SqlKind.AS) {
                // Case 2 & 3: Aliased expressions - SELECT name AS name0 or SELECT COUNT(*) AS count0
                SqlBasicCall asCall = (SqlBasicCall) selectItem;
                SqlNode sourceExpr = asCall.operand(0);

                if (sourceExpr instanceof SqlIdentifier) {
                    // Case 2: Simple column with auto-generated alias - SELECT name AS name0 -> SELECT name
                    SqlTypeName fieldType = resolveFieldType(sourceExpr.toString(), tableSchema);
                    SqlNode processedItem = isTimestampType(fieldType) ? processTimestampColumnWithCasting(sourceExpr) : sourceExpr;
                    newSelectList.add(processedItem);
                } else {
                    // Case 3: Complex expression with alias - SELECT COUNT(*) AS count0 (keep alias)
                    newSelectList.add(selectItem);
                }
            } else {
                // Case 4: Complex expressions without alias - SELECT COUNT(*), name + 1 (keep as is)
                newSelectList.add(selectItem);
            }
        }

        // Clone and replace select list
        SqlSelect newSelect = (SqlSelect) select.clone(select.getParserPosition());
        newSelect.setSelectList(newSelectList);
        return newSelect;
    }

    /** Resolves the {@link SqlTypeName} for a field name from the schema, or null if not found. */
    private SqlTypeName resolveFieldType(String fieldName, RelDataType tableSchema) {
        for (RelDataTypeField field : tableSchema.getFieldList()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field.getType().getSqlTypeName();
            }
        }
        return null;
    }

    /** Returns true if the type is TIMESTAMP or TIMESTAMP_WITH_LOCAL_TIME_ZONE. */
    private boolean isTimestampType(SqlTypeName typeName) {
        return typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    /** Expands {@code *} to explicit column references so individual timestamp fields can be cast. */
    private SqlNodeList expandSelectList(final SqlSelect select, final RelDataType tableSchema) {
        SqlNodeList expandedList = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode selectItem : select.getSelectList()) {
            if (selectItem instanceof SqlIdentifier && ((SqlIdentifier) selectItem).isStar()) {
                // Expand * to all columns from the table schema
                for (RelDataTypeField field : tableSchema.getFieldList()) {
                    expandedList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
                }
            } else {
                expandedList.add(selectItem);
            }
        }
        return expandedList;
    }

    /**
     * Wraps a timestamp column in {@code CAST(column AS VARCHAR) AS column}.
     * Prevents Vertica from exporting timestamp/timestamptz as INT96.
     */
    private SqlNode processTimestampColumnWithCasting(SqlNode columnNode) {
        String columnName = ((SqlIdentifier) columnNode).getSimple();
        // Escape embedded double-quotes in column names
        String quotedColumn = QUOTE_CHARS + columnName.replace("\"", "\"\"") + QUOTE_CHARS;

        // Parse only the CAST expression, not the alias
        String castExpression = "CAST(" + quotedColumn + " AS VARCHAR)";

        try {
            SqlNode castNode = SqlParser.create(castExpression).parseExpression();

            // Add alias separately using Calcite's AS operator
            return SqlStdOperatorTable.AS.createCall(
                    SqlParserPos.ZERO,
                    castNode,
                    new SqlIdentifier(columnName, SqlParserPos.ZERO)
            );
        }
        catch (Exception e) {
            LOGGER.warn("Failed to cast column '{}', using column without casting. Error: {}",
                    columnName, e.getMessage());
            return columnNode; // Fallback to original
        }
    }

    /**
     * Converts FETCH NEXT/OFFSET ROWS syntax to Vertica's LIMIT/OFFSET.
     * Calcite's VerticaSqlDialect incorrectly emits FETCH NEXT instead of LIMIT.
     */
    private String convertFetchToVerticaLimit(final SqlSelect select) {
        String sql = select.toString();
        if (select.getOffset() != null) {
            // Convert "OFFSET n ROW(S)" to "OFFSET n" (Calcite emits singular ROW when count=1)
            sql = sql.replaceAll("OFFSET (\\d+) ROWS?", "OFFSET $1");
        }

        if (select.getFetch() != null) {
            // Convert "FETCH NEXT n ROW(S) ONLY" to "LIMIT n" (Calcite emits singular ROW when count=1)
            sql = sql.replaceAll("FETCH NEXT (\\d+) ROWS? ONLY", "LIMIT $1");
        }
        return sql;
    }

    /**
     * Replaces WHERE clause literals with {@code {paramN}} placeholders for StringTemplate.
     * SELECT clause literals are left unchanged.
     */
    private SqlSelect createParameterizedQuery(SqlSelect select) {
        AtomicInteger paramIndex = new AtomicInteger(0);

        if (select.getWhere() != null) {
            SqlShuttle parameterize = new SqlShuttle() {
                @Override
                public SqlNode visit(SqlDynamicParam param) {
                    String paramName = "param" + paramIndex.getAndIncrement();
                    return new SqlDynamicParam(param.getIndex(), param.getParserPosition()) {
                        @Override
                        public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
                            writer.literal("{" + paramName + "}");
                        }
                    };
                }
            };
            // Apply shuttle ONLY to the WHERE clause to avoid corrupting FETCH/OFFSET literals
            SqlNode transformedWhere = select.getWhere().accept(parameterize);
            select.setWhere(transformedWhere);
        }
        return select;
    }

    /**
     * Binds {@link SubstraitTypeAndValue} parameters to StringTemplate attributes with type-appropriate formatting.
     * TINYINT/SMALLINT cases may not be hit (Substrait promotes them to INTEGER) but are included for safety.
     */
    @VisibleForTesting
    void handleDataTypesForSqlTemplate(final List<SubstraitTypeAndValue> accumulator, ST sqlTemplate) {
        for (int i = 0; i < accumulator.size(); i++) {
            SubstraitTypeAndValue typeAndValue = accumulator.get(i);
            String paramName = "param" + i;
            SqlTypeName sqlTypeName = typeAndValue.getType();

            switch (sqlTypeName) {
                case BOOLEAN:
                    sqlTemplate.add(paramName, Boolean.parseBoolean(typeAndValue.getValue().toString()) ? 1 : 0);
                    break;
                case TINYINT:
                    sqlTemplate.add(paramName, Byte.parseByte(typeAndValue.getValue().toString()));
                    break;
                case SMALLINT:
                    sqlTemplate.add(paramName, Short.parseShort(typeAndValue.getValue().toString()));
                    break;
                case INTEGER:
                    sqlTemplate.add(paramName, Integer.parseInt(typeAndValue.getValue().toString()));
                    break;
                case BIGINT:
                    sqlTemplate.add(paramName, Long.parseLong(typeAndValue.getValue().toString()));
                    break;
                case FLOAT:
                    sqlTemplate.add(paramName, Float.parseFloat(typeAndValue.getValue().toString()));
                    break;
                case DOUBLE:
                    sqlTemplate.add(paramName, Double.parseDouble(typeAndValue.getValue().toString()));
                    break;
                case DECIMAL:
                    sqlTemplate.add(paramName, new BigDecimal(typeAndValue.getValue().toString()));
                    break;
                case DATE:
                    LocalDate date = LocalDate.parse(typeAndValue.getValue().toString());
                    sqlTemplate.add(paramName, "DATE '" + date + "'");
                    break;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalDateTime parsedTimestamp;
                    try {
                        if (typeAndValue.getValue().toString().contains("T")) {
                            parsedTimestamp = LocalDateTime.parse(typeAndValue.getValue().toString());
                        } else {
                            // Supports all Calcite timestamp formats, including fractional seconds with 0-9 digits
                            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                                    .optionalStart()
                                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                                    .optionalEnd()
                                    .toFormatter();

                            parsedTimestamp = LocalDateTime.parse(typeAndValue.getValue().toString(), formatter);
                        }
                    }
                    catch (DateTimeParseException e) {
                        LOGGER.error("Can't handle timestamp format: {}, value class: {}", typeAndValue.getType(), typeAndValue.getValue().getClass().getName());
                        throw new AthenaConnectorException(String.format("Can't handle timestamp format: %s, value class: %s", typeAndValue.getType(), typeAndValue.getValue().getClass().getName()),
                                ErrorDetails.builder()
                                        .errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString())
                                        .build());
                    }
                    String formattedTimestamp = VERTICA_TIMESTAMP_LITERAL_FORMATTER.format(parsedTimestamp);
                    sqlTemplate.add(paramName, "TIMESTAMP '" + formattedTimestamp + "'");
                    break;
                case VARCHAR:
                    String val = "'" + typeAndValue.getValue().toString().replace("'", "''") + "'";
                    sqlTemplate.add(paramName, val);
                    break;
                case VARBINARY:
                    sqlTemplate.add(paramName, bytesToVerticaHexLiteral((byte[]) typeAndValue.getValue()));
                    break;
                default:
                    LOGGER.error("Can't handle type: {}, {}", typeAndValue.getType(), typeAndValue.getType());
                    throw new AthenaConnectorException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), typeAndValue.getType()),
                            ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
            }
        }
    }
}
