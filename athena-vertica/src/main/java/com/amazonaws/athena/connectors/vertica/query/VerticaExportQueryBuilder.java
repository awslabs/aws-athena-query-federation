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
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.SubstraitTypeAndValue;
import com.amazonaws.athena.connectors.jdbc.visitor.SubstraitAccumulatorVisitor;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.Types;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.vertica.query.PredicateBuilder.getFromClauseWithSplit;

public class VerticaExportQueryBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerticaExportQueryBuilder.class);
    private static final String TEMPLATE_NAME = "templateVerticaExportQuery";
    private static final String QPT_TEMPLATE_NAME = "templateVerticaExportQPTQuery";
    private static final String SUBSTRAIT_TEMPLATE_NAME = "templateVerticaExportSubstraitQuery";
    private static final String TEMPLATE_FIELD = "builder";
    private static final String QUOTE_CHARS = "\"";
    private final ST query;
    private String s3ExportBucket;
    private String table;
    private String queryID;
    private String colNames;
    private String constraintValues;
    private String queryFromPlan;
    private String preparedStatementSQL;

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

    static String getSubstraitTemplateName() {
        return SUBSTRAIT_TEMPLATE_NAME;
    }

    public String getTable(){return table;}

    public String getQueryFromPlan() {
        return queryFromPlan;
    }

    public VerticaExportQueryBuilder fromTable(String schemaName, String tableName)
    {
        this.table = getFromClauseWithSplit(schemaName, tableName);
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
            }
            else {
                colN.append(f.getName()).append(",");
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
                    String val = "'" + typeAndValue.getValue() + "'";
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
        ST castFieldST = new ST("CAST(<name> AS VARCHAR) AS <name>");
        castFieldST.add("name", name);
        return castFieldST.render();
    }

    //build the Vertica SQL to set the AWS Region
    public String buildSetAwsRegionSql(String awsRegion)
    {
        if (awsRegion == null || awsRegion.equals("")) { 
            awsRegion = "us-east-1"; 
        }
        ST regionST=  new ST("ALTER SESSION SET AWSRegion='<defaultRegion>'") ;
        regionST.add("defaultRegion", awsRegion);
        return regionST.render();
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

    public VerticaExportQueryBuilder withQueryPlan(QueryPlan queryPlan, SqlDialect sqlDialect,
                                                   final String schema,
                                                   final String table, final Schema tableSchema) {
        try {
            String base64EncodedPlan = queryPlan.getSubstraitPlan();
            SqlNode sqlNode = SubstraitSqlUtils.deserializeSubstraitPlan(base64EncodedPlan, sqlDialect, schema, table, tableSchema);
            List<SubstraitTypeAndValue> accumulator = new ArrayList<>();

            SqlSelect select;

            if (!(sqlNode instanceof SqlSelect)) {
                throw new RuntimeException("Unsupported Query Type. Only SELECT Query is supported.");
            }

            select = (SqlSelect) sqlNode;

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");
            
            // Parse select list to handle aliases properly
            SqlNodeList selectList = select.getSelectList();
            List<String> projectedColumns = new ArrayList<>();
            
            for (SqlNode selectItem : selectList) {
                String columnExpression = null;
                
                if (selectItem instanceof SqlIdentifier) {
                    // Simple column reference: SELECT name FROM users
                    SqlIdentifier identifier = (SqlIdentifier) selectItem;
                    columnExpression = identifier.getSimple();
                }
                else if (selectItem instanceof SqlBasicCall &&
                        ((SqlBasicCall) selectItem).getOperator().getKind() == SqlKind.AS) {
                    // Handle aliased expressions: SELECT name AS name0 FROM table_name
                    SqlBasicCall asCall = (SqlBasicCall) selectItem;
                    SqlNode sourceExpr = asCall.operand(0);

                    if (sourceExpr instanceof SqlIdentifier) {
                        SqlIdentifier sourceId = (SqlIdentifier) sourceExpr;
                        columnExpression = sourceId.getSimple();
                    } else {
                        // Complex expression with alias SELECT COUNT(*) from table_name - keep as is
                        //
                        columnExpression = selectItem.toSqlString(sqlDialect).getSql();
                    }
                }
                else {
                    // Handle other complex expressions
                    columnExpression = selectItem.toSqlString(sqlDialect).getSql();
                }
                
                if (columnExpression != null) {
                    projectedColumns.add(columnExpression);
                }
            }
            
            sql.append(String.join(", ", projectedColumns));

            if (projectedColumns.isEmpty()) {
                sql.append("null");
            }
            sql.append(" FROM ");
            sql.append(getFromClauseWithSplit(schema, table));

            SqlNode whereClause = select.getWhere();
            List<String> clauses = new ArrayList<>();
            if (whereClause != null) {
                whereClause.accept(new SubstraitAccumulatorVisitor(accumulator, Collections.emptyMap(), tableSchema));
                clauses.add(whereClause.toSqlString(sqlDialect).getSql());
            }

            if (!clauses.isEmpty()) {
                sql.append(" WHERE ");
                sql.append(String.join(" AND ", clauses));
            }

            if (select.getOrderList() != null) {
                List<String> orderParts = new ArrayList<>();
                for (SqlNode orderExpr : select.getOrderList()) {
                    String part = orderExpr.toSqlString(sqlDialect).getSql();
                    orderParts.add(part);
                }
                String orderByClause = " ORDER BY " + String.join(", ", orderParts);
                sql.append(orderByClause);
            }

            String limit = select.getFetch() == null ? null : select.getFetch().toSqlString(sqlDialect).getSql();
            String offset = select.getOffset() == null ? null : select.getOffset().toSqlString(sqlDialect).getSql();

            if (limit != null) {
                sql.append(appendLimitOffsetWithValue(limit, offset));
            }
            ST sqlTemplate = new ST(sql.toString());
            System.out.println("accumulator size " + accumulator.size());
            System.out.println("Accumulator " + accumulator);
            for (int i = 0; i < accumulator.size(); i++) {
                SubstraitTypeAndValue typeAndValue = accumulator.get(i);
                System.out.println("typeAndValue " + typeAndValue);
                SqlTypeName sqlTypeName = typeAndValue.getType();
                String colName = typeAndValue.getColumnName();
                switch (sqlTypeName)
                {
                    case TINYINT:
                        sqlTemplate.add(colName, Byte.parseByte(typeAndValue.getValue().toString()));
                        break;
                    case SMALLINT:
                        sqlTemplate.add(colName, Short.parseShort(typeAndValue.getValue().toString()));
                        break;
                    case INTEGER:
                        sqlTemplate.add(colName, Integer.parseInt(typeAndValue.getValue().toString()));
                        break;
                    case BIGINT:
                        sqlTemplate.add(colName,Long.parseLong(typeAndValue.getValue().toString()));
                        break;
                    case FLOAT:
                        sqlTemplate.add(colName,Float.parseFloat(typeAndValue.getValue().toString()));
                        break;
                    case DOUBLE:
                        sqlTemplate.add(colName,Double.parseDouble(typeAndValue.getValue().toString()));
                        break;
                    case DECIMAL:
                        sqlTemplate.add(colName, new BigDecimal(typeAndValue.getValue().toString()));
                        break;
                    case DATE:
                        if (typeAndValue.getValue() instanceof DateString) {
                            sqlTemplate.add(colName, (int) LocalDate.parse(typeAndValue.getValue().toString()).toEpochDay());
                        }
                        else if (typeAndValue.getValue() instanceof TimestampString) {
                            sqlTemplate.add(colName, LocalDateTime.parse(typeAndValue.getValue().toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                        }
                        else {
                            throw new AthenaConnectorException(
                                    String.format("Can't handle date format: %s", typeAndValue.getType()),
                                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
                        }
                        break;

                    case VARCHAR:
                        String val = "'" + typeAndValue.getValue() + "'";
                        sqlTemplate.add(colName, val);
                        break;
                    case VARBINARY:
                        sqlTemplate.add(colName, typeAndValue.toString().getBytes());
                        break;

                    default:
                        throw new AthenaConnectorException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), typeAndValue.getType()),
                                ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());                }
            }
            this.queryFromPlan = sqlTemplate.render();
            return this;
        }
        catch (Exception e) {
            LOGGER.error("withQueryPlan failed", e);
            throw new RuntimeException("withQueryPlan Error", e);
        }
    }

    public String build()
    {
        Validate.notNull(s3ExportBucket, "s3ExportBucket can not be null.");
        Validate.notNull(table != null ? table : (preparedStatementSQL != null ? preparedStatementSQL : queryFromPlan), "table can not be null.");
        Validate.notNull(queryID, "queryID can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    private String appendLimitOffsetWithValue(String limit, String offset)
    {
        if (offset != null && !offset.equals("0")) {
            return String.format(" OFFSET %s LIMIT %s", offset, limit);
        }
        return String.format(" LIMIT %s", limit);
    }
}
