/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaUtilsTest
{

    @Test
    public void UnsupportedTypeTest()
    {
        List<Document> docs = new ArrayList<>();
        Document unsupported = new Document();
        unsupported.put("unsupported_col1", new UnsupportedType());
        docs.add(unsupported);

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockDatabase.getCollection(any())).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(docs.iterator()));

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("unsupported_col1").getType()));
    }

    @Test
    public void basicMergeTest()
    {
        List<String> list = new ArrayList<>();
        list.add("test");
        list.add("test");
        list.add("test");

        Document struct1 = new Document();
        struct1.put("struct_col1", 1);
        struct1.put("struct_col2", "string");
        struct1.put("struct_col3", 1.0D);

        Document struct2 = new Document();
        struct2.put("struct_col1", 1);
        struct2.put("struct_col2", "string");
        struct2.put("struct_col3", 1);
        struct2.put("struct_col4", 2.0F);

        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col5", list);
        doc1.put("col6", struct1);
        docs.add(doc1);

        Document doc2 = new Document();
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col4", 1.0F);
        doc2.put("col6", struct2);
        docs.add(doc2);

        Document doc3 = new Document();
        doc3.put("col1", 1);
        doc3.put("col2", "string");
        doc3.put("col4", 1);
        doc3.put("col5", list);
        docs.add(doc3);

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockDatabase.getCollection(any())).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(docs.iterator()));

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(6, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col5").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col5").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(fields.get("col6").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(1).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(2).getType()));
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(3).getType()));
    }

    @Test
    public void emptyListTest()
    {
        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        List<String> emptyList = new ArrayList<>();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col4", emptyList);
        docs.add(doc1);

        Document doc2 = new Document();
        List<Integer> list2 = new ArrayList<>();
        list2.add(100);
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col3", 1.0D);
        doc2.put("col4", list2);
        docs.add(doc2);

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockDatabase.getCollection(any())).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(docs.iterator()));

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(4, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getChildren().get(0).getType()));
    }
}
