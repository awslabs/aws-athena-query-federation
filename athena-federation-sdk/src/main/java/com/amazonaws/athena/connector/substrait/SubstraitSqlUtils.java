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
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
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
        final Rel rel = protoPlan.getRelations(0).getRoot().getInput();
        final ReadRel readRel = SubstraitRelUtils.getReadRel(rel);

        if (readRel == null || !readRel.hasBaseSchema()) {
            throw new RuntimeException("Unable to extract base table schema from Substrait plan");
        }

        return convertSubstraitTypeToArrowSchema(readRel.getBaseSchema());
    }

    private static Schema convertSubstraitTypeToArrowSchema(final NamedStruct namedStruct)
    {
        final List<Field> fields = new ArrayList<>();
        final Type.Struct struct = namedStruct.getStruct();

        for (int i = 0; i < struct.getTypesCount(); i++) {
            final String name = i < namedStruct.getNamesCount() ? namedStruct.getNames(i) : "field_" + i;
            final ArrowType type = convertSubstraitTypeToArrowType(struct.getTypes(i));
            fields.add(new Field(name, new FieldType(true, type, null), null));
        }

        return new Schema(fields);
    }

    private static ArrowType convertSubstraitTypeToArrowType(final Type type)
    {
        if (type.hasBool()) {
            return ArrowType.Bool.INSTANCE;
        }
        if (type.hasI8()) {
            return new ArrowType.Int(8, true);
        }
        if (type.hasI16()) {
            return new ArrowType.Int(16, true);
        }
        if (type.hasI32()) {
            return new ArrowType.Int(32, true);
        }
        if (type.hasI64()) {
            return new ArrowType.Int(64, true);
        }
        if (type.hasFp32()) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if (type.hasFp64()) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (type.hasString()) {
            return ArrowType.Utf8.INSTANCE;
        }
        if (type.hasBinary()) {
            return ArrowType.Binary.INSTANCE;
        }
        if (type.hasDate()) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        if (type.hasTime()) {
            return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
        }
        if (type.hasTimestamp()) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
        if (type.hasDecimal()) {
            return new ArrowType.Decimal(type.getDecimal().getPrecision(), type.getDecimal().getScale(), 128);
        }
        if (type.hasList()) {
            return new ArrowType.List();
        }
        if (type.hasMap()) {
            return new ArrowType.Map(false);
        }
        if (type.hasStruct()) {
            return new ArrowType.Struct();
        }
        if (type.hasPrecisionTimestamp()) {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        }

        LOGGER.warn("Unsupported Substrait type: {}, defaulting to Utf8", type);
        return ArrowType.Utf8.INSTANCE;
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
}
