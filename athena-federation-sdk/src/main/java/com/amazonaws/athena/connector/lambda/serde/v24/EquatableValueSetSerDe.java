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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class EquatableValueSetSerDe
        extends TypedSerDe<ValueSet>
{
    private static final String VALUE_BLOCK_FIELD = "valueBlock";
    private static final String WHITELIST_FIELD = "whiteList";
    private static final String NULL_ALLOWED_FIELD = "nullAllowed";

    private final BlockSerDe blockSerDe;

    public EquatableValueSetSerDe(BlockSerDe blockSerDe)
    {
        this.blockSerDe = requireNonNull(blockSerDe, "blockSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, ValueSet valueSet)
            throws IOException
    {
        EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;

        jgen.writeFieldName(VALUE_BLOCK_FIELD);
        blockSerDe.serialize(jgen, equatableValueSet.getValueBlock());

        jgen.writeBooleanField(WHITELIST_FIELD, equatableValueSet.isWhiteList());
        jgen.writeBooleanField(NULL_ALLOWED_FIELD, equatableValueSet.nullAllowed);
    }

    @Override
    public EquatableValueSet doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, VALUE_BLOCK_FIELD);
        Block valueBlock = blockSerDe.deserialize(jparser);

        boolean whiteList = getNextBoolField(jparser, WHITELIST_FIELD);
        boolean nullAllowed = getNextBoolField(jparser, NULL_ALLOWED_FIELD);

        return new EquatableValueSet(valueBlock, whiteList, nullAllowed);
    }
}
