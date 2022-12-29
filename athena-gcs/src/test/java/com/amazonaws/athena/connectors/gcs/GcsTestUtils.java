///*-
// * #%L
// * athena-google-bigquery
// * %%
// * Copyright (C) 2019 - 2022 Amazon Web Services
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//package com.amazonaws.athena.connectors.gcs;
//
//import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
//import com.amazonaws.athena.storage.StorageTable;
//import com.amazonaws.athena.storage.TableListResult;
//import com.amazonaws.athena.storage.common.StorageObject;
//import com.amazonaws.athena.storage.common.StoragePartition;
//import com.amazonaws.athena.storage.gcs.GroupSplit;
//import com.amazonaws.athena.storage.gcs.StorageSplit;
//import org.apache.arrow.vector.types.Types;
//import org.apache.arrow.vector.types.pojo.ArrowType;
//import org.apache.arrow.vector.types.pojo.Field;
//import org.apache.arrow.vector.types.pojo.FieldType;
//import org.apache.arrow.vector.types.pojo.Schema;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//public class GcsTestUtils
//{
//    public static final String BOOL_FIELD_NAME_1 = "bool1";
//    public static final String INTEGER_FIELD_NAME_1 = "int1";
//    public static final String STRING_FIELD_NAME_1 = "string1";
//    public static final String FLOAT_FIELD_NAME_1 = "float1";
//
//    private GcsTestUtils()
//    {
//    }
//
//    public static final String PROJECT_1_NAME = "testProject";
//
//    //Returns a list of mocked Datasets.
//    static List<String> getDatasetList()
//    {
//        List<String> datasetList = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            datasetList.add("dataset" + i);
//        }
//        return datasetList;
//    }
//
//    // Returns a list of mocked Tables
//    static TableListResult getTableList()
//    {
//        List<String> tableList = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            tableList.add("table" + i);
//        }
//        List<StorageObject> storageObjects = tableList.stream()
//                .map(table -> StorageObject.builder().setTabletName(table).build())
//                .collect(Collectors.toList());
//        return new TableListResult(storageObjects, "testToken");
//    }
//
//    //Returns the schema by returning a list of fields in Google BigQuery Format.
//    static StorageTable getTestSchemaFields()
//    {
//        List<Field> fields = getFields();
//        Map<String, String> map = new HashMap<>();
//        map.put("bucketName", "test");
//        map.put("partitioned_table_base", "test");
//        return new StorageTable("test", "test", map, fields, false);
//    }
//
//    static List<Field> getFields()
//    {
//        List<Field> fields = new ArrayList<>();
//        fields.add(new Field(BOOL_FIELD_NAME_1, new FieldType(true, Types.MinorType.BIT.getType(), null), null));
//        fields.add(new Field(INTEGER_FIELD_NAME_1, new FieldType(true, Types.MinorType.INT.getType(), null), null));
//        fields.add(new Field(STRING_FIELD_NAME_1, new FieldType(true, Types.MinorType.VARCHAR.getType(), null), null));
//        fields.add(new Field(FLOAT_FIELD_NAME_1, new FieldType(true, new ArrowType.Decimal(5, 5, 128), null), null));
//        return fields;
//    }
//
//    public static Schema getTestSchema()
//    {
//        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
//        StorageTable storageTable = getTestSchemaFields();
//
//        for (Field field : storageTable.getFields()) {
//            schemaBuilder.addField(field);
//        }
//        return schemaBuilder.build();
//    }
//
//    public static List<StorageSplit> getSplits()
//    {
//        List<StorageSplit> splitList = new ArrayList<>();
//        List<GroupSplit> groupSplits = new ArrayList<>();
//        splitList.add(new StorageSplit("test", groupSplits));
//        splitList.add(new StorageSplit("test1", groupSplits));
//        return splitList;
//    }
//
//    static org.apache.arrow.vector.types.pojo.Schema getBlockTestSchema()
//    {
//        return SchemaBuilder.newBuilder()
//                .addBitField(BOOL_FIELD_NAME_1)
//                .addIntField(INTEGER_FIELD_NAME_1)
//                .addStringField(STRING_FIELD_NAME_1)
//                .addFloat8Field(FLOAT_FIELD_NAME_1)
//                .build();
//    }
//
//    public static List<StoragePartition> getStoragePartition() {
//        return List.of(StoragePartition.builder().bucketName("test").objectNames(List.of("test")).location("test").recordCount(10L).children(List.of()).build(),
//                StoragePartition.builder().bucketName("test").objectNames(List.of("test")).location("test").recordCount(10L).children(List.of()).build());
//    }
//}
