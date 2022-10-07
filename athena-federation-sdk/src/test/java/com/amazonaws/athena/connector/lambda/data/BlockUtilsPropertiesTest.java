package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2022 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.helpers.ValuesGenerator;
import com.amazonaws.athena.connector.lambda.data.helpers.CustomFieldVector;
import com.amazonaws.athena.connector.lambda.data.helpers.FieldsGenerator;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jqwik.api.*;

import java.util.LinkedHashMap;
import java.util.Map;


public class BlockUtilsPropertiesTest {

    private static final Logger logger = LoggerFactory.getLogger(BlockUtilsTest.class);

    @Provide
    protected Arbitrary<Field> fieldLowRecursion() {
        FieldsGenerator fieldsGenerator = new FieldsGenerator(2);
        return fieldsGenerator.field();
    }

    @Provide
    protected Arbitrary<Field> fieldHighRecursion() {
        FieldsGenerator fieldsGenerator = new FieldsGenerator(5);
        return fieldsGenerator.field();
    }

    protected FieldResolver getFieldResolver(Schema schema) {
        return new ArrowToArrowResolver();
    }

    protected Object getValue(FieldVector vector, CustomFieldVector customFieldVector, int pos, FieldResolver resolver) {
        if (vector.getMinorType().equals(MinorType.MAP)) {
            return resolver.getFieldValue(vector.getField(), vector.getObject(pos));
        }
        else {
            return vector.getObject(pos);
        }
    }

    // Using the default number of tries here (1000)
    // With a higher recursion level, we will not be able to cover all possible combinations
    // This will do a random sampling of scenarios
    // The purpose of this test is to cover depth
    @Property
    boolean setComplexValuesSetsAllFieldsCorrectlyGivenAnyInputHighRecursion(@ForAll("fieldHighRecursion") Field field) {
        return setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput(field);
    }

    // By setting a lower recursion level, we are able to cover all edge cases with 2000 tries
    // The purpose of this test is to cover all the different combinations of fields (but with less depth)
    @Property(tries=2000, edgeCases=EdgeCasesMode.FIRST)
    boolean setComplexValuesSetsAllFieldsCorrectlyGivenAnyInputAllCombinations(@ForAll("fieldLowRecursion") Field field) {
        return setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput(field);
    }

    private boolean setComplexValuesSetsAllFieldsCorrectlyGivenAnyInput(Field field) {
        ValuesGenerator generator = new ValuesGenerator();
        RootAllocator allocator = new RootAllocator();
        FieldVector vector = field.createVector(allocator);
        CustomFieldVector customFieldVector = new CustomFieldVector(field);
        generator.generateValues(field, vector, customFieldVector);

        Schema schema = new Schema(java.util.List.of(field));
        VectorSchemaRoot inputSchemaRoot = new VectorSchemaRoot(schema, java.util.List.of(vector), 1);

        int valueCount = inputSchemaRoot.getVector(0).getValueCount();
        VectorSchemaRoot outputSchemaRoot = VectorSchemaRoot.create(inputSchemaRoot.getSchema(), allocator);
        outputSchemaRoot.setRowCount(1);
        FieldResolver resolver = getFieldResolver(schema);
        for (int i = 0; i < valueCount; i++) {
            if (field.getType().isComplex()) {
                BlockUtils.setComplexValue(
                    outputSchemaRoot.getVector(0), i, resolver, getValue(vector, customFieldVector, i, resolver)
                );
            }
            else {
                BlockUtils.setValue(outputSchemaRoot.getVector(0), i, getValue(vector, customFieldVector, i, resolver));
            }
        }

        outputSchemaRoot.getVector(0).setValueCount(valueCount);
        if (inputSchemaRoot.equals(outputSchemaRoot)) {
            logger.debug(
                "Matched for Schema:\n\t"
                + inputSchemaRoot.getSchema().toString() + "\n"
                + "with FieldVectors:\n\t"
                + inputSchemaRoot.getFieldVectors().toString() + "\n"
            );
        }
        else {
            logger.error(
                "DID NOT MATCH\n"
                + "Input Schema:\n\t"
                + inputSchemaRoot.getSchema().toString() + "\n"
                + "Output Schema:\n\t"
                + outputSchemaRoot.getSchema().toString() + "\n"
                + "Input FieldVectors:\n\t"
                + inputSchemaRoot.getFieldVectors().toString() + "\n"
                + "Output FieldVectors:\n\t"
                + outputSchemaRoot.getFieldVectors().toString() + "\n"
            );
            return false;
        }

        return true;
    }
}

class ArrowToArrowResolver implements FieldResolver {

    // Needed for maps since IdentityHashMap does not maintain order and LinkedHashMap does not use reference equality
    class KeyWithEquality {
        public Object key;

        KeyWithEquality(Object key) {
            this.key = key;
        }

        public boolean equals(Object o) {
            return this.key == o;
        }
    }

    @Override
    public Object getFieldValue(Field field, Object originalValue) {
        if (originalValue.getClass().equals(KeyWithEquality.class)) {
            originalValue = ((KeyWithEquality) originalValue).key;
        }

        if (field.getType().getTypeID() == ArrowType.Map.TYPE_TYPE) {
            JsonStringArrayList input;
            if (originalValue.getClass().equals(JsonStringHashMap.class)) {
                input = (JsonStringArrayList) ((Map) originalValue).get(field.getName());
            }
            else {
                input = (JsonStringArrayList) originalValue;
            }

            if (input == null) {
                return null;
            }

            Map<Object, Object> outputMap = new LinkedHashMap<Object, Object>();
            for (int i = 0; i <  input.size(); i++) {
                JsonStringHashMap inputMap = (JsonStringHashMap) input.get(i);
                outputMap.put(new KeyWithEquality(inputMap.get(MapVector.KEY_NAME)), inputMap.get(MapVector.VALUE_NAME));
            }
            return outputMap;
        }

        if (originalValue.getClass().equals(JsonStringHashMap.class) || originalValue.getClass().equals(Map.class)) {
            Object fieldValue = ((Map) originalValue).get(field.getName());
            return fieldValue;
        }
        return originalValue;
    }

    @Override
    public Object getMapKey(Field field, Object originalValue) {
        return getFieldValue(field, originalValue);
    }

    @Override
    public Object getMapValue(Field field, Object originalValue) {
        return getFieldValue(field, originalValue);
    }
}


