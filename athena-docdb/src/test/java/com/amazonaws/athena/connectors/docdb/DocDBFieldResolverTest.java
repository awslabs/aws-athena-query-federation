/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.mongodb.DBRef;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocDBFieldResolverTest
{
    private static final String TEST_LIST_FIELD = "myList";
    private static final String TEST_STRUCT_FIELD = "myStruct";
    private static final String TEST_NESTED_FIELD = "nestedField";
    private static final String TEST_COMPLEX_LIST = "complexList";
    private static final String TEST_FIELD1 = "field1";
    private static final String TEST_ID = "123";
    private static final String ITEM_FIELD = "item";
    private static final String VALUE_STRING = "value";
    private static final String OTHER_DB = "otherDb";
    private static final String OTHER_COLL = "otherColl";

    private final FieldResolver resolver = DocDBFieldResolver.DEFAULT_FIELD_RESOLVER;

    @Test
    public void getFieldValue_withListField_returnsListValue()
    {
        Field listField = new Field(TEST_LIST_FIELD, 
            FieldType.nullable(Types.MinorType.LIST.getType()),
                List.of(new Field(ITEM_FIELD,
                        FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                        null)));

        Document doc = new Document();
        List<String> list = Arrays.asList("item1", "item2", "item3");
        doc.put(TEST_LIST_FIELD, list);

        Object result = resolver.getFieldValue(listField, doc);
        assertNotNull("Result should not be null", result);
        assertEquals(list, result);
    }

    @Test
    public void getFieldValue_withSimpleDocument_returnsFieldValue()
    {
        Field structField = new Field(TEST_STRUCT_FIELD, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        Document outerDoc = new Document();
        outerDoc.put(TEST_STRUCT_FIELD, VALUE_STRING);

        Object result = resolver.getFieldValue(structField, outerDoc);
        assertNotNull("Result should not be null", result);
        assertEquals(VALUE_STRING, result);
    }

    @Test
    public void getFieldValue_withDBRef_returnsDBRefFields()
    {
        Field idField = new Field("_id",
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);
        Field dbField = new Field("_db",
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);
        Field refField = new Field("_ref",
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        DBRef dbRef = mock(DBRef.class);
        when(dbRef.getId()).thenReturn(TEST_ID);
        when(dbRef.getDatabaseName()).thenReturn(OTHER_DB);
        when(dbRef.getCollectionName()).thenReturn(OTHER_COLL);

        assertEquals(TEST_ID, resolver.getFieldValue(idField, dbRef));
        assertEquals(OTHER_DB, resolver.getFieldValue(dbField, dbRef));
        assertEquals(OTHER_COLL, resolver.getFieldValue(refField, dbRef));
    }

    @Test
    public void getFieldValue_withNestedDocument_returnsNestedDocument()
    {
        List<Field> nestedFields = new ArrayList<>();
        nestedFields.add(new Field(TEST_NESTED_FIELD, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null));
        Field structField = new Field(TEST_STRUCT_FIELD, 
            FieldType.nullable(Types.MinorType.STRUCT.getType()), 
            nestedFields);

        Document innerDoc = new Document();
        innerDoc.put(TEST_NESTED_FIELD, "nestedValue");
        Document outerDoc = new Document();
        outerDoc.put(TEST_STRUCT_FIELD, innerDoc);

        Object result = resolver.getFieldValue(structField, outerDoc);
        assertNotNull("Result should not be null", result);
        assertEquals(innerDoc, result);
    }

    @Test
    public void getFieldValue_withComplexList_returnsComplexList()
    {
        List<Field> structFields = new ArrayList<>();
        structFields.add(new Field(TEST_FIELD1, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null));
        Field itemField = new Field(ITEM_FIELD, 
            FieldType.nullable(Types.MinorType.STRUCT.getType()), 
            structFields);
        Field listField = new Field(TEST_COMPLEX_LIST, 
            FieldType.nullable(Types.MinorType.LIST.getType()),
                List.of(itemField));

        Document doc1 = new Document(TEST_FIELD1, "value1");
        Document doc2 = new Document(TEST_FIELD1, "value2");
        List<Document> list = Arrays.asList(doc1, doc2);
        
        Document mainDoc = new Document();
        mainDoc.put(TEST_COMPLEX_LIST, list);

        Object result = resolver.getFieldValue(listField, mainDoc);
        assertNotNull("Result should not be null", result);
        assertEquals(list, result);
    }

    @Test
    public void getFieldValue_withInvalidValue_throwsRuntimeException()
    {
        Field field = new Field("test", 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        try {
            resolver.getFieldValue(field, 123);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertTrue("Exception message should contain Expected LIST or Document type but found",
                    ex.getMessage().contains("Expected LIST or Document type but found"));
        }
    }
} 
