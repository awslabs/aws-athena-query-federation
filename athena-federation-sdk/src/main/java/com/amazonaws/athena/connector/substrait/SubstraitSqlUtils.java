/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.substrait;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting Substrait plans to SQL and extracting schema information.
 * <p>
 * Provides methods to:
 * - Convert Substrait plans (serialized as base64 strings) to Calcite SqlNode representations
 * - Extract Arrow Schema from Substrait plans
 * - Convert between Calcite RelDataType and Apache Arrow types
 * <p>
 * This class uses Apache Calcite as an intermediate representation to bridge Substrait plans
 * and SQL dialects, enabling connectors to work with query pushdown capabilities.
 */

public final class SubstraitSqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitSqlUtils.class);

    private SubstraitSqlUtils()
    {
    }

    public static SqlNode getSqlNodeFromSubstraitPlan(final String planString, final SqlDialect sqlDialect)
    {
        try {
            final Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
            return getSqlNodeFromSubstraitPlan(protoPlan, sqlDialect);
        }
        catch (final Exception e) {
            LOGGER.error("Failed to parse Substrait plan", e);
            throw new RuntimeException("Failed to parse Substrait plan", e);
        }
    }

    private static SqlNode getSqlNodeFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        final RelNode node = getRelNodeFromSubstraitPlan(protoPlan, sqlDialect);
        final RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
        return converter.visitRoot(node).asStatement();
    }

    public static Schema getTableSchemaFromSubstraitPlan(final String planString, final SqlDialect sqlDialect)
    {
        try {
            final Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
            return getTableSchemaFromSubstraitPlan(protoPlan, sqlDialect);
        }
        catch (final Exception e) {
            LOGGER.error("Failed to extract table schema from Substrait plan", e);
            throw new RuntimeException("Failed to extract table schema from Substrait plan", e);
        }
    }

    private static Schema getTableSchemaFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        final RelNode node = getRelNodeFromSubstraitPlan(protoPlan, sqlDialect);
        final RelDataType relDataType = node.getRowType();
        return convertRelDataTypeToArrowSchema(relDataType);
    }

    private static RelNode getRelNodeFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        try {
            final ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter();
            final io.substrait.plan.Plan substraitPlan = protoPlanConverter.from(protoPlan);

            final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(
                    SimpleExtension.loadDefaults(),
                    new SqlTypeFactoryImpl(sqlDialect.getTypeSystem())
            );

            return substraitToCalcite.convert(substraitPlan.getRoots().get(0).getInput());
        }
        catch (final Exception e) {
            LOGGER.error("Failed to convert from Substrait plan to RelNode", e);
            throw new RuntimeException("Failed to convert from Substrait plan to RelNode", e);
        }
    }

    /**
     * Converts a Calcite RelDataType to an Arrow Schema.
     *
     * @param relDataType The Calcite RelDataType to convert
     * @return The corresponding Arrow Schema
     */
    private static Schema convertRelDataTypeToArrowSchema(final RelDataType relDataType)
    {
        final List<Field> fields = new ArrayList<>();

        for (final RelDataTypeField relField : relDataType.getFieldList()) {
            final String fieldName = relField.getName();
            final RelDataType fieldType = relField.getType();
            final ArrowType arrowType = convertSqlTypeToArrowType(fieldType.getSqlTypeName());
            final boolean nullable = fieldType.isNullable();

            final Field field = new Field(fieldName, new FieldType(nullable, arrowType, null), null);
            fields.add(field);
        }

        return new Schema(fields);
    }

    /**
     * Converts a Calcite SqlTypeName to an Arrow ArrowType.
     *
     * @param sqlTypeName The Calcite SqlTypeName to convert
     * @return The corresponding Arrow ArrowType
     */
    private static ArrowType convertSqlTypeToArrowType(final SqlTypeName sqlTypeName)
    {
        switch (sqlTypeName) {
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case REAL:
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case DECIMAL:
                return new ArrowType.Decimal(38, 10, 128); // Default precision and scale
            case CHAR:
            case VARCHAR:
                return ArrowType.Utf8.INSTANCE;
            case BINARY:
            case VARBINARY:
                return ArrowType.Binary.INSTANCE;
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return new ArrowType.Interval(IntervalUnit.DAY_TIME);
            case ARRAY:
                return new ArrowType.List();
            case MAP:
                return new ArrowType.Map(false);
            case ROW:
            case MULTISET:
                return new ArrowType.Struct();
            default:
                LOGGER.warn("Unsupported SQL type: {}, defaulting to Utf8", sqlTypeName);
                return ArrowType.Utf8.INSTANCE;
        }
    }
}
