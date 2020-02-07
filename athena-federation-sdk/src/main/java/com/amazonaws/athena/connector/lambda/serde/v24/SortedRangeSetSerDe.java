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

import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class SortedRangeSetSerDe extends TypedSerDe<ValueSet>
{
    private static final String TYPE_FIELD = "type";
    private static final String RANGES_FIELD = "ranges";
    private static final String NULL_ALLOWED_FIELD = "nullAllowed";

    private final ArrowTypeSerDe arrowTypeSerDe;
    private final RangeSerDe rangeSerDe;

    public SortedRangeSetSerDe(ArrowTypeSerDe arrowTypeSerDe, RangeSerDe rangeSerDe)
    {
        super(SortedRangeSet.class);
        this.arrowTypeSerDe = requireNonNull(arrowTypeSerDe, "arrowTypeSerDe is null");
        this.rangeSerDe = requireNonNull(rangeSerDe, "rangeSerde is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, ValueSet valueSet)
            throws IOException
    {
        SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;

        jgen.writeFieldName(TYPE_FIELD);
        arrowTypeSerDe.serialize(jgen, sortedRangeSet.getType());

        jgen.writeFieldName(RANGES_FIELD);
        jgen.writeStartArray();
        for (Range range : sortedRangeSet.getOrderedRanges()) {
            rangeSerDe.serialize(jgen, range);
        }
        jgen.writeEndArray();

        jgen.writeBooleanField(NULL_ALLOWED_FIELD, sortedRangeSet.isNullAllowed());
    }

    @Override
    public SortedRangeSet doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, TYPE_FIELD);
        ArrowType type = arrowTypeSerDe.deserialize(jparser);

        assertFieldName(jparser, RANGES_FIELD);
        validateArrayStart(jparser);
        ImmutableList.Builder<Range> rangesList = ImmutableList.builder();
        while (jparser.nextToken() != JsonToken.END_ARRAY) {
            validateObjectStart(jparser.getCurrentToken());
            rangesList.add(rangeSerDe.doDeserialize(jparser));
            validateObjectEnd(jparser);
        }

        boolean nullAllowed = getNextBoolField(jparser, NULL_ALLOWED_FIELD);

        return SortedRangeSet.copyOf(type, rangesList.build(), nullAllowed);
    }
}
