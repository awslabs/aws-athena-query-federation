/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.BaseSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ConstraintsSerDe
        extends BaseSerDe<Constraints>
{
    private static final String SUMMARY_FIELD = "summary";

    private final ValueSetSerDe valueSetSerDe;

    public ConstraintsSerDe(ValueSetSerDe valueSetSerDe)
    {
        this.valueSetSerDe = requireNonNull(valueSetSerDe, "valueSetSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, Constraints constraints)
            throws IOException
    {
        jgen.writeObjectFieldStart(SUMMARY_FIELD);
        for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
            jgen.writeFieldName(entry.getKey());
            valueSetSerDe.serialize(jgen, entry.getValue());
        }
        jgen.writeEndObject();
    }

    @Override
    public Constraints doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, SUMMARY_FIELD);
        validateObjectStart(jparser.nextToken());
        ImmutableMap.Builder<String, ValueSet> summaryMap = ImmutableMap.builder();
        while (jparser.nextToken() != JsonToken.END_OBJECT) {
            String column = jparser.getCurrentName();
            summaryMap.put(column, valueSetSerDe.deserialize(jparser));
        }

        return new Constraints(summaryMap.build());
    }
}
