/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.deltalake.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeltaLogActionTest extends TestCase {

    String arrayInnerRecordName = "bag";
    String arrayElementName = "bag";

    String mapInnerRecordName = "map";
    String mapKeyName = "key";
    String mapValueName = "value";


    private Group getGroupFromMap(Map<String, String> map) {
        List<Type> innerFields = Arrays.asList(
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, mapKeyName),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, mapValueName)
        );
        List<Type> fields = Arrays.asList(
                new GroupType(Type.Repetition.REPEATED, mapInnerRecordName, innerFields)
        );
        MessageType schema = new MessageType("groupName", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        for (Map.Entry<String, String> entry: map.entrySet()) {
            Group innerRecord = new SimpleGroupFactory(new MessageType(mapInnerRecordName, innerFields)).newGroup();
            innerRecord.add(mapKeyName, entry.getKey());
            innerRecord.add(mapValueName, entry.getValue());
            record.add(mapInnerRecordName, innerRecord);
        }
        return record;
    }

    private Group getGroupFromList(List<String> list) {
        List<Type> innerFields = Arrays.asList(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, arrayElementName)
        );
        List<Type> fields = Arrays.asList(
                new GroupType(Type.Repetition.REPEATED, arrayInnerRecordName, innerFields)
        );
        MessageType schema = new MessageType("groupName", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        for (String element: list) {
            Group innerRecord = new SimpleGroupFactory(new MessageType(arrayInnerRecordName, innerFields)).newGroup();
            innerRecord.add(arrayElementName, element);
            record.add(arrayInnerRecordName, innerRecord);
        }
        return record;
    }

    @Test
    public void testAddFileFromJsonString() throws JsonProcessingException {
        // Given
        String json = "{\"path\":\"year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet\",\"partitionValues\":{\"year\":\"2021\",\"month\":\"6\",\"day\":\"4\"},\"size\":1236,\"modificationTime\":1647223053000,\"dataChange\":true}";
        DeltaLogAction.AddFile expectedAddFile = new DeltaLogAction.AddFile(
            "year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet",
            ImmutableMap.<String, String>builder().put("year", "2021").put("month", "6").put("day", "4").build()
        );
        // When
        DeltaLogAction.AddFile addFile = DeltaLogAction.AddFile.fromJsonString(json);
        // Then
        assertEquals(expectedAddFile, addFile);
    }

    @Test
    public void testAddFileFromParquet() {
        // Given
        List<Type> fields = Arrays.asList(
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "path"),
            new GroupType(Type.Repetition.OPTIONAL, "partitionValues", Arrays.asList(
                new GroupType(Type.Repetition.REPEATED, mapInnerRecordName, Arrays.asList(
                    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, mapKeyName),
                    new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, mapValueName)
                ))
            )),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "size"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "modificationTime"),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "dataChange"),
            new GroupType(Type.Repetition.OPTIONAL, "tags", Arrays.asList(
                new GroupType(Type.Repetition.REPEATED, mapInnerRecordName, Arrays.asList(
                    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, mapKeyName),
                    new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, mapValueName)
                ))
            )),
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "stats"),
            new GroupType(Type.Repetition.OPTIONAL, "partitionValues_parsed", Arrays.asList(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "year"),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "month"),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "day")
            ))
        );
        MessageType schema = new MessageType("add", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        record.add("path", Binary.fromCharSequence("year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet"));
        record.add("partitionValues", getGroupFromMap(ImmutableMap.of("year", "2021", "month", "6", "day", "4")));

        DeltaLogAction.AddFile expectedAddFile = new DeltaLogAction.AddFile(
            "year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet",
            ImmutableMap.<String, String>builder().put("year", "2021").put("month", "6").put("day", "4").build()
        );
        // When
        DeltaLogAction.AddFile addFile = DeltaLogAction.AddFile.fromParquet(record);
        // Then
        assertEquals(expectedAddFile, addFile);
    }

    @Test
    public void testRemoveFileFromJsonString() throws JsonProcessingException {
        // Given
        String json = "{\"path\":\"year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet\",\"partitionValues\":{\"year\":\"2021\",\"month\":\"6\",\"day\":\"4\"},\"size\":1236,\"modificationTime\":1647223053000,\"dataChange\":true}";
        DeltaLogAction.RemoveFile expectedRemoveFile = new DeltaLogAction.RemoveFile(
        "year=2021/month=6/day=4/part-00000-123d7d3b-1237-1233-1231-123a84250a46.c000.snappy.parquet"
        );
        // When
        DeltaLogAction.RemoveFile addFile = DeltaLogAction.RemoveFile.fromJsonString(json);
        // Then
        assertEquals(expectedRemoveFile, addFile);
    }


    @Test
    public void testMetaDataFromJsonString() throws JsonProcessingException {
        // Given
        String json = "{\"id\":\"123b8ea6-1238-1234-1230-123791ddf5a1\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"event_date\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"year\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"month\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"day\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[\"year\",\"month\",\"day\"],\"configuration\":{},\"createdTime\":1624703081508}";
        DeltaLogAction.MetaData expectedMetaData = new DeltaLogAction.MetaData(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"event_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                Arrays.asList("year", "month", "day")
        );
        // When
        DeltaLogAction.MetaData addFile = DeltaLogAction.MetaData.fromJsonString(json);
        // Then
        assertEquals(expectedMetaData, addFile);
    }

    @Test
    public void testMetaDataFromParquet() {
        // Given
        String schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"event_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"year\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"month\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"day\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
        List<String> partitionColumns = Arrays.asList("year", "month", "day");
        List<Type> fields = Arrays.asList(
            new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "schemaString"),
            new GroupType(Type.Repetition.OPTIONAL, "partitionColumns", Collections.singletonList(
                new GroupType(Type.Repetition.REPEATED, arrayInnerRecordName, Collections.singletonList(
                    new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, arrayElementName)
                ))
            ))
        );
        MessageType schema = new MessageType("add", fields);
        Group record = new SimpleGroupFactory(schema).newGroup();
        record.add("schemaString", Binary.fromCharSequence(schemaString));
        record.add("partitionColumns", getGroupFromList(partitionColumns));

        DeltaLogAction.MetaData expectedMetaData = new DeltaLogAction.MetaData(
            schemaString,
            partitionColumns
        );
        // When
        DeltaLogAction.MetaData metaData = DeltaLogAction.MetaData.fromParquet(record);
        // Then
        assertEquals(expectedMetaData, metaData);
    }
}
