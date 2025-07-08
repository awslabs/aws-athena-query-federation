/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v6;

import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public final class QueryPlanSerDe
{
    private static final String SUBSTRAIT_VERSION_FIELD = "substraitVersion";
    private static final String SUBSTRAIT_PLAN_FIELD = "substraitPlan";
    private static final String SQL_CONFORMANCE_FIELD = "sqlConformance";

    private QueryPlanSerDe() {}

    public static final class Serializer extends BaseSerializer<QueryPlan>
    {
        public Serializer()
        {
            super(QueryPlan.class);
        }

        @Override
        public void doSerialize(QueryPlan queryPlan, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(SUBSTRAIT_VERSION_FIELD, queryPlan.getSubstraitVersion());
            jgen.writeStringField(SUBSTRAIT_PLAN_FIELD, queryPlan.getSubstraitPlan());
            jgen.writeStringField(SQL_CONFORMANCE_FIELD, queryPlan.getSqlConformance());
        }
    }

    public static final class Deserializer extends BaseDeserializer<QueryPlan>
    {
        public Deserializer()
        {
            super(QueryPlan.class);
        }

        @Override
        public QueryPlan doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String substraitVersion = getNextStringField(jparser, SUBSTRAIT_VERSION_FIELD);
            String substraitPlan = getNextStringField(jparser, SUBSTRAIT_PLAN_FIELD);
            String sqlConformance = getNextStringField(jparser, SQL_CONFORMANCE_FIELD);

            return new QueryPlan(substraitVersion, substraitPlan, sqlConformance);
        }
    }
}
