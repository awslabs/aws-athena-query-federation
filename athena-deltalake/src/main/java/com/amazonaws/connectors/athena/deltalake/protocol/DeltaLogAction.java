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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.example.data.Group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Container for all Delta Log Action POJO
 */
public interface DeltaLogAction
{
    /**
     * POJO for "addFile" Delta Log Action
     */
    class AddFile implements DeltaLogAction
    {
        public String path;
        public Map<String, String> partitionValues;

        public AddFile(String path, Map<String, String> partitionValues)
        {
            this.path = path;
            this.partitionValues = partitionValues;
        }

        public static AddFile fromJsonString(String json) throws JsonProcessingException
        {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String path = actualObj.get("path").asText();
            Map<String, String> partitionValues = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> partitionValuesFields = actualObj.get("partitionValues").fields();
            while (partitionValuesFields.hasNext()) {
                Map.Entry<String, JsonNode> entry = partitionValuesFields.next();
                partitionValues.put(entry.getKey(), entry.getValue().textValue());
            }
            return new AddFile(path, partitionValues);
        }

        public static AddFile fromParquet(Group group)
        {
            String path = group.getBinary("path", 0).toStringUsingUTF8();
            Group partitionValuesGroup = group.getGroup("partitionValues", 0);
            Map<String, String> partitionValues = new HashMap<>();
            int mapSize = partitionValuesGroup.getFieldRepetitionCount(0);
            for (int i = 0; i < mapSize; i++) {
                String partitionName = partitionValuesGroup.getGroup(0, i).getBinary("key", 0).toStringUsingUTF8();
                String partitionValue = partitionValuesGroup.getGroup(0, i).getBinary("value", 0).toStringUsingUTF8();
                partitionValues.put(partitionName, partitionValue);
            }
            return new AddFile(path, partitionValues);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AddFile addFile = (AddFile) o;
            return path.equals(addFile.path) && partitionValues.equals(addFile.partitionValues);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path, partitionValues);
        }
    }

    /**
     * POJO for "removeFile" Delta Log Action
     */

    class RemoveFile implements DeltaLogAction
    {
        String path;

        public RemoveFile(String path)
        {
            this.path = path;
        }

        public static DeltaLogAction.RemoveFile fromJsonString(String json) throws JsonProcessingException
        {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String path = actualObj.get("path").asText();
            return new DeltaLogAction.RemoveFile(path);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemoveFile that = (RemoveFile) o;
            return path.equals(that.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path);
        }
    }

    /**
     * POJO for "metadata" Delta Log Action
     */
    class MetaData implements DeltaLogAction
    {
        public String schemaString;
        public List<String> partitionColumns;

        public MetaData(String schemaString, List<String> partitionColumns)
        {
            this.schemaString = schemaString;
            this.partitionColumns = partitionColumns;
        }

        public static DeltaLogAction.MetaData fromJsonString(String json) throws JsonProcessingException
        {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String schemaString = actualObj.get("schemaString").asText();
            List<String> partitionColumnsGroup = new ArrayList<>();
            Iterator<JsonNode> partitionColumnsElements = actualObj.get("partitionColumns").elements();
            while (partitionColumnsElements.hasNext()) {
                JsonNode partitionColumn = partitionColumnsElements.next();
                partitionColumnsGroup.add(partitionColumn.asText());
            }
            return new DeltaLogAction.MetaData(schemaString, partitionColumnsGroup);
        }

        public static DeltaLogAction.MetaData fromParquet(Group group)
        {
            String schemaString = group.getBinary("schemaString", 0).toStringUsingUTF8();
            List<String> partitionColumns = new ArrayList<>();
            Group partitionColumnsGroup = group.getGroup("partitionColumns", 0);
            for (int i = 0; i < partitionColumnsGroup.getFieldRepetitionCount(0); i++) {
                partitionColumns.add(partitionColumnsGroup.getGroup(0, i).getBinary(0, 0).toStringUsingUTF8());
            }
            return new DeltaLogAction.MetaData(schemaString, partitionColumns);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeltaLogAction.MetaData metaData = (DeltaLogAction.MetaData) o;
            return schemaString.equals(metaData.schemaString) && partitionColumns.equals(metaData.partitionColumns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaString, partitionColumns);
        }
    }
}
