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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for working with Calcite's abstract syntax tree representation of Substrait plans.
 */
public final class SubstraitSqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitSqlUtils.class);

    private SubstraitSqlUtils()
    {
    }

    public static SqlNode deserializeSubstraitPlan(String planString, SqlDialect sqlDialect)
    {
        try {
            LOGGER.debug("substrait plan: {}", planString);

            // 1. Deserialize Substrait plan from base64
            Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);

            // 2. Convert proto plan to Substrait plan object
            ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter();
            io.substrait.plan.Plan substraitPlan = protoPlanConverter.from(protoPlan);

            // 3. Convert Substrait plan to Calcite RelNode with schema extraction
            SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(
                    SimpleExtension.loadDefaults(),
                    new SqlTypeFactoryImpl(sqlDialect.getTypeSystem())
            );
            RelNode node = substraitToCalcite.convert(substraitPlan.getRoots().get(0).getInput());

            // 4. Convert RelNode to SQL using RelToSqlConverter
            RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
            
            return converter.visitRoot(node).asStatement();
        }
        catch (Exception e) {
            LOGGER.error("Failed to parse Substrait plan", e);
            throw new RuntimeException("Failed to parse Substrait plan", e);
        }
    }
}
