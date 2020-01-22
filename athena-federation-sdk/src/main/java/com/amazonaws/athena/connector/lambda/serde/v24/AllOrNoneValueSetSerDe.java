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

import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AllOrNoneValueSetSerDe
        extends TypedSerDe<ValueSet>
{
    private static final String TYPE_FIELD = "type";
    private static final String ALL_FIELD = "all";
    private static final String NULL_ALLOWED_FIELD = "nullAllowed";

    private final ArrowTypeSerDe arrowTypeSerDe;

    public AllOrNoneValueSetSerDe(ArrowTypeSerDe arrowTypeSerDe)
    {
        this.arrowTypeSerDe = requireNonNull(arrowTypeSerDe, "arrowTypeSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, ValueSet valueSet)
            throws IOException
    {
        AllOrNoneValueSet allOrNoneValueSet = (AllOrNoneValueSet) valueSet;

        jgen.writeFieldName(TYPE_FIELD);
        arrowTypeSerDe.serialize(jgen, allOrNoneValueSet.getType());

        jgen.writeBooleanField(ALL_FIELD, allOrNoneValueSet.isAll());
        jgen.writeBooleanField(NULL_ALLOWED_FIELD, allOrNoneValueSet.isNullAllowed());
    }

    @Override
    public AllOrNoneValueSet doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, TYPE_FIELD);
        ArrowType type = arrowTypeSerDe.deserialize(jparser);

        boolean all = getNextBoolField(jparser, ALL_FIELD);
        boolean nullAllowed = getNextBoolField(jparser, NULL_ALLOWED_FIELD);

        return new AllOrNoneValueSet(type, all, nullAllowed);
    }
}
