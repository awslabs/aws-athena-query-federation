/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver;
import com.amazonaws.athena.connector.lambda.data.BlockUtilsPropertiesTest;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.helpers.CustomFieldVector;
import com.amazonaws.athena.connector.lambda.data.helpers.FieldsGenerator;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.FieldVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import net.jqwik.api.*;

class DDBFieldResolverTest extends BlockUtilsPropertiesTest {

    private static final Logger logger = LoggerFactory.getLogger(DDBFieldResolverTest.class);

    @Override @Provide
    protected Arbitrary<Field> fieldLowRecursion() {
        FieldsGenerator fieldsGenerator = new FieldsGenerator(2, false);
        return fieldsGenerator.field();
    }

    @Override @Provide
    protected Arbitrary<Field> fieldHighRecursion() {
        FieldsGenerator fieldsGenerator = new FieldsGenerator(5, false);
        return fieldsGenerator.field();
    }

    @Override
    protected FieldResolver getFieldResolver(Schema schema) {
        DDBRecordMetadata recordMetadata = new DDBRecordMetadata(schema);
        DynamoDBFieldResolver resolver = new DynamoDBFieldResolver(recordMetadata);
        return (FieldResolver) resolver;
    }

    @Override
    protected Object getValue(FieldVector vector, CustomFieldVector customFieldVector, int pos, FieldResolver resolver) {
        return ((List<Object>) (customFieldVector.objList)).get(pos);
    }
}
