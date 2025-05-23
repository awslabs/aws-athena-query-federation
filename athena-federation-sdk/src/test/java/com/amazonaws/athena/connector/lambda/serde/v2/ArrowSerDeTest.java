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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

public class ArrowSerDeTest
{
    // we need to have a specific fall back for MinorType.TIMESTAMPMILLITZ since it doesn't have default mapping to ArrowType
    // AND the simple names are different (i.e. TIMESTAMPMILLITZ != Timestamp).
    // MinorType.DECIMAL which is a similar case of no default mapping to ArrowType does not need this as
    // MinorType name and ArrowType name are the same so simple fall back of enum name works.
    private static final ImmutableMap<SupportedTypes, String> FALL_BACK_ARROW_TYPE_CLASS = ImmutableMap.of(
        SupportedTypes.TIMESTAMPMILLITZ, ArrowType.Timestamp.class.getSimpleName(),
        SupportedTypes.TIMESTAMPMICROTZ, ArrowType.Timestamp.class.getSimpleName()
    );

    @Test
    public void testSupportedTypesHaveSerializers()
    {
        ArrowTypeSerDe.Serializer arrowTypeSerializer = new ArrowTypeSerDe.Serializer();
        Map<String, TypedSerializer<ArrowType>> delegateSerDeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        delegateSerDeMap.putAll(arrowTypeSerializer.getDelegateSerDeMap());
        for (SupportedTypes supportedType : SupportedTypes.values()) {
            String arrowTypeClass;
            try {
                ArrowType arrowType = supportedType.getArrowMinorType().getType();
                arrowTypeClass = arrowType.getClass().getSimpleName();
            }
            catch (UnsupportedOperationException e) {
                // fall back to enum name
                arrowTypeClass = FALL_BACK_ARROW_TYPE_CLASS.getOrDefault(supportedType, supportedType.name());
            }
            assertTrue("No serializer for supported type " + supportedType + " with ArrowType " + arrowTypeClass, delegateSerDeMap.containsKey(arrowTypeClass));
        }
    }

    @Test
    public void testSupportedTypesHaveDeserializers()
    {
        ArrowTypeSerDe.Deserializer arrowTypeDeserializer = new ArrowTypeSerDe.Deserializer();
        Map<String, TypedDeserializer<ArrowType>> delegateSerDeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        delegateSerDeMap.putAll(arrowTypeDeserializer.getDelegateSerDeMap());
        for (SupportedTypes supportedType : SupportedTypes.values()) {
            String arrowTypeClass;
            try {
                ArrowType arrowType = supportedType.getArrowMinorType().getType();
                arrowTypeClass = arrowType.getClass().getSimpleName();
            }
            catch (UnsupportedOperationException e) {
                // fall back to enum name
                arrowTypeClass = FALL_BACK_ARROW_TYPE_CLASS.getOrDefault(supportedType, supportedType.name());
            }
            assertTrue("No deserializer for supported type " + supportedType + " with ArrowType " + arrowTypeClass, delegateSerDeMap.containsKey(arrowTypeClass));
        }
    }
}
