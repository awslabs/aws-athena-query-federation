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

import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.serde.BaseSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class RangeSerDe extends BaseSerDe<Range>
{
    private static final String LOW_FIELD = "low";
    private static final String HIGH_FIELD = "high";

    private final MarkerSerDe markerSerDe;

    public RangeSerDe(MarkerSerDe markerSerDe)
    {
        this.markerSerDe = requireNonNull(markerSerDe, "markerSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, Range range)
            throws IOException
    {
        jgen.writeFieldName(LOW_FIELD);
        markerSerDe.serialize(jgen, range.getLow());

        jgen.writeFieldName(HIGH_FIELD);
        markerSerDe.serialize(jgen, range.getHigh());
    }

    @Override
    public Range doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, LOW_FIELD);
        Marker low = markerSerDe.deserialize(jparser);

        assertFieldName(jparser, HIGH_FIELD);
        Marker high = markerSerDe.deserialize(jparser);

        return new Range(low, high);
    }
}
