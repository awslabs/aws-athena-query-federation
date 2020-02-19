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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

public class ArrowSerDeTest
{
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
                arrowTypeClass = supportedType.name();
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
                arrowTypeClass = supportedType.name();
            }
            assertTrue("No deserializer for supported type " + supportedType + " with ArrowType " + arrowTypeClass, delegateSerDeMap.containsKey(arrowTypeClass));
        }
    }
}
