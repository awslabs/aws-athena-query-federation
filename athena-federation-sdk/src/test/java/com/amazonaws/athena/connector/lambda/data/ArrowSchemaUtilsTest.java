/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import java.util.function.Function;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ArrowSchemaUtilsTest {

    @Test
    public void testRemapTypeInStruct() {
        // Verify that this type is changed later
        DictionaryEncoding bsdfDictionaryEncoding = new DictionaryEncoding(1111, false, new ArrowType.Int(32, false));
        Map<String, String> bsdfMeta = ImmutableMap.of("11111111", "2222222222");
        FieldType bsdfType = new FieldType(true, new ArrowType.Utf8(), bsdfDictionaryEncoding, bsdfMeta);
        Field bsdf = new Field("bsdf", bsdfType, null);

        // Verify that this type is unchanged later
        DictionaryEncoding csdfDictionaryEncoding = new DictionaryEncoding(2222, true, new ArrowType.Int(32, true));
        Map<String, String> csdfMeta = ImmutableMap.of("3333333333", "4444444444");
        FieldType csdfType = new FieldType(true, new ArrowType.Decimal(12, 3, 32), csdfDictionaryEncoding, csdfMeta);
        Field csdf = new Field("csdf", csdfType, null);

        // Inner struct
        DictionaryEncoding asdfDictionaryEncoding = new DictionaryEncoding(33333, false, new ArrowType.Int(64, false));
        Map<String, String> asdfMeta = ImmutableMap.of("55555555", "66666666");
        FieldType asdfFieldType = new FieldType(true, new ArrowType.Struct(), asdfDictionaryEncoding, asdfMeta);
        Field asdf = new Field("asdf", asdfFieldType, ImmutableList.of(bsdf, csdf));

        // Outer struct that also repeats bsdf and csdf on the outside
        DictionaryEncoding aasdfDictionaryEncoding = new DictionaryEncoding(44444, true, new ArrowType.Int(64, true));
        Map<String, String> aasdfMeta = ImmutableMap.of("7777", "88888");
        FieldType aasdfFieldType = new FieldType(true, new ArrowType.Struct(), aasdfDictionaryEncoding, aasdfMeta);
        Field aasdf = new Field("aasdf", aasdfFieldType, ImmutableList.of(asdf, bsdf, csdf));

        Function<ArrowType, ArrowType> arrowTypeMapper = arrowType -> {
            if (arrowType instanceof ArrowType.Utf8) {
                return new ArrowType.Decimal(24, 6, 32);
            } else {
                return arrowType;
            }
        };

        // Run remapArrowTypesWithinField
        Field outputField = ArrowSchemaUtils.remapArrowTypesWithinField(aasdf, arrowTypeMapper);

        // This verifies most of the fields
        assertEquals(
            "aasdf: Struct[dictionary: 44444]<" +
                "asdf: Struct[dictionary: 33333]<" +
                    "bsdf: Decimal(24, 6, 32)[dictionary: 1111], " +
                    "csdf: Decimal(12, 3, 32)[dictionary: 2222]" +
                ">, " +
                "bsdf: Decimal(24, 6, 32)[dictionary: 1111], " +
                "csdf: Decimal(12, 3, 32)[dictionary: 2222]" +
            ">",
            outputField.toString());

        // Now verify the fields that aren't covered by toString()
        assertTrue(outputField.getFieldType().isNullable());
        assertEquals(aasdfMeta, outputField.getFieldType().getMetadata());

        Field outputAsdf = outputField.getChildren().get(0);
        assertTrue(outputAsdf.getFieldType().isNullable());
        assertEquals(asdfMeta, outputAsdf.getFieldType().getMetadata());

        Field innerOutputBsdf = outputField.getChildren().get(0).getChildren().get(0);
        assertTrue(innerOutputBsdf.getFieldType().isNullable());
        assertEquals(bsdfMeta, innerOutputBsdf.getFieldType().getMetadata());

        Field innerOutputCsdf = outputField.getChildren().get(0).getChildren().get(1);
        assertTrue(innerOutputCsdf.getFieldType().isNullable());
        assertEquals(csdfMeta, innerOutputCsdf.getFieldType().getMetadata());

        Field outputBsdf = outputField.getChildren().get(1);
        assertTrue(outputBsdf.getFieldType().isNullable());
        assertEquals(bsdfMeta, outputBsdf.getFieldType().getMetadata());

        Field outputCsdf = outputField.getChildren().get(2);
        assertTrue(outputCsdf.getFieldType().isNullable());
        assertEquals(csdfMeta, outputCsdf.getFieldType().getMetadata());
    }
}

