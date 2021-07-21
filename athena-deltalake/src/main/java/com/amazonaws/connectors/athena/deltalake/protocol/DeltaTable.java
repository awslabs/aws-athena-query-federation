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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DeltaTable {
    String path;
    String bucket;
    Configuration conf;

    public String deltaLogDirectoryKey() {
        return path + "/_delta_log";
    }

    AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

    public static interface DeltaAction {}

    public static class LastCheckpoint {
        long version;
        long size;
        Optional<Long> parts;

        public LastCheckpoint(long version, long size, Optional<Long> parts) {
            this.version = version;
            this.size = size;
            this.parts = parts;
        }
    }

    public static class DeltaTableSnapshot {
        public Collection<AddFile> files;
        public MetaData metaData;

        public DeltaTableSnapshot(Collection<AddFile> files, MetaData metaData) {
            this.files = files;
            this.metaData = metaData;
        }
    }

    public static class RemoveFile implements DeltaAction {
        String path;

        public RemoveFile(String path) {
            this.path = path;
        }

        public static RemoveFile fromJsonString(String json) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String path = actualObj.get("path").asText();
            return new RemoveFile(path);
        }
    }

    public static class AddFile implements DeltaAction {
        public String path;
        public Map<String, String> partitionValues;

        public AddFile(String path, Map<String, String> partitionValues) {
            this.path = path;
            this.partitionValues = partitionValues;
        }

        public static AddFile fromJsonString(String json) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String path = actualObj.get("path").asText();
            Map<String, String> partitionValues = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> partitionValuesFields = actualObj.get("partitionValues").fields();
            while(partitionValuesFields.hasNext()) {
                Map.Entry<String, JsonNode> entry = partitionValuesFields.next();
                partitionValues.put(entry.getKey(), entry.getValue().textValue());
            }
            return new AddFile(path, partitionValues);
        }

        public static AddFile fromParquet(Group group) {
            String path = group.getBinary("path", 0).toStringUsingUTF8();
            Group partitionValuesGroup = group.getGroup("partitionValues", 0);
            Map<String, String> partitionValues = new HashMap<>();
            int mapSize = partitionValuesGroup.getFieldRepetitionCount(0);
            for (int i = 0; i<mapSize; i++ ) {
                String partitionName = partitionValuesGroup.getGroup(0, i).getBinary("key", 0).toStringUsingUTF8();
                String partitionValue = partitionValuesGroup.getGroup(0, i).getBinary("value", 0).toStringUsingUTF8();
                partitionValues.put(partitionName, partitionValue);
            }
            return new AddFile(path, partitionValues);
        }

    }

    public static class MetaData implements DeltaAction {
        public String schemaString;
        public List<String> partitionColumns;

        public MetaData(String schemaString, List<String> partitionColumns) {
            this.schemaString = schemaString;
            this.partitionColumns = partitionColumns;
        }

        public static MetaData fromJsonString(String json) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(json);
            String path = actualObj.get("version").asText();
            Map<String, String> partitionValues = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> partitionValuesFields = actualObj.get("partitionValues").fields();
            while(partitionValuesFields.hasNext()) {
                Map.Entry<String, JsonNode> entry = partitionValuesFields.next();
                partitionValues.put(entry.getKey(), entry.getValue().textValue());
            }
            return new MetaData(path, Collections.emptyList());
        }

        public static MetaData fromParquet(Group group) {
            String schemaString = group.getBinary("schemaString", 0).toStringUsingUTF8();
            List<String> partitionColumns = new ArrayList<>();
            Group partitionColumnsGroup = group.getGroup("partitionColumns", 0);
            for(int i = 0; i< partitionColumnsGroup.getFieldRepetitionCount(0); i ++) {
                partitionColumns.add(partitionColumnsGroup.getGroup(0, i).getBinary(0, 0).toStringUsingUTF8());
            }
            return new MetaData(schemaString, partitionColumns);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetaData metaData = (MetaData) o;
            return schemaString.equals(metaData.schemaString) && partitionColumns.equals(metaData.partitionColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaString, partitionColumns);
        }
    }

    protected LastCheckpoint getLastCheckpoint() throws IOException {
        String lastCheckpointFileKey = deltaLogDirectoryKey() + "/_last_checkpoint";
        BufferedReader lastCheckpointFile = openS3File(bucket, lastCheckpointFileKey);
        String lastCheckpointString = lastCheckpointFile.readLine();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(lastCheckpointString);
        long version = actualObj.get("version").asLong();
        long size = actualObj.get("size").asLong();
        Optional<Long> parts = Optional.ofNullable(actualObj.get("parts")).map(JsonNode::asLong);
        return new LastCheckpoint(version, size, parts);
    }


    public DeltaTable(String path, String bucket) {
        this.path = path;
        this.bucket = bucket;
        Configuration conf = new Configuration();
        conf.setLong("fs.s3a.multipart.size", 104857600);
        conf.setInt("fs.s3a.multipart.threshold", Integer.MAX_VALUE);
        conf.setBoolean("fs.s3a.impl.disable.cache", true);
        conf.set("fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore");
        this.conf = conf;

    }

    private BufferedReader openS3File(String bucket, String key)
    {
        if (amazonS3.doesObjectExist(bucket, key)) {
            S3Object obj = amazonS3.getObject(bucket, key);
            return new BufferedReader(new InputStreamReader(obj.getObjectContent()));
        }
        return null;
    }


    static public List<String> getCheckpointFiles(LastCheckpoint lastCheckpoint) throws IOException {
        String checkpointVersion = StringUtils.leftPad(String.valueOf(lastCheckpoint.version), 20, '0');
        List<String> result = new ArrayList<>();
        if (lastCheckpoint.parts.isPresent()) {
            long parts = lastCheckpoint.parts.get();
            String partTotal = StringUtils.leftPad(String.valueOf(parts), 10, '0');
            for (long part=1 ; part<=parts ; part++) {
                String partNumber = StringUtils.leftPad(String.valueOf(part), 10, '0');
                String fileName = String.format("%s.checkpoint.%s.%s.parquet", checkpointVersion, partNumber, partTotal);
                result.add(fileName);
            }
        } else {
            String fileName = checkpointVersion + ".checkpoint.parquet";
            result.add(fileName);
        }
        return result;
    }

    public List<String> getJsonDeltaLogs(String lastCheckpoint) {
        ListObjectsV2Request listRequest = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(deltaLogDirectoryKey() + "/")
                .withStartAfter(deltaLogDirectoryKey() + "/" + lastCheckpoint);
        ListObjectsV2Result result = amazonS3.listObjectsV2(listRequest);
        List<String> deltaLogs = new ArrayList<>();
        for(S3ObjectSummary object: result.getObjectSummaries()) {
            if(object.getKey().endsWith(".json")) deltaLogs.add(object.getKey());
        }
        return deltaLogs;
    }

    public List<DeltaAction> readDeltaLogs(List<String> deltaLogsFileKeys) throws IOException {
        List<DeltaAction> deltaActions = new ArrayList<>();
        for (String deltaLogsFileKey: deltaLogsFileKeys) {
            BufferedReader deltaLogsFile = openS3File(bucket, deltaLogsFileKey);
            String line;
            while ((line = deltaLogsFile.readLine()) != null) {
                String deltaLogString = deltaLogsFile.readLine();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode deltaLogJson = mapper.readTree(deltaLogString);
                DeltaAction deltaAction = parseJsonDeltaAction(deltaLogJson);
                if (deltaAction != null) deltaActions.add(deltaAction);
            }

        }
        return deltaActions;
    }


    public DeltaAction parseDeltaAction(Group deltaAction) {
        if (deltaAction.getFieldRepetitionCount("add") > 0) {
            return AddFile.fromParquet(deltaAction.getGroup("add", 0));
        } else if (deltaAction.getFieldRepetitionCount("metaData") > 0) {
            return MetaData.fromParquet(deltaAction.getGroup("metaData", 0));
        } else return null;
    }

    public DeltaAction parseJsonDeltaAction(JsonNode deltaAction) throws JsonProcessingException {
        if (deltaAction.has("add")) {
            return AddFile.fromJsonString(deltaAction.get("add").toString());
        } else if (deltaAction.has("metaData")) {
            return MetaData.fromJsonString(deltaAction.get("metaData").toString());
        } else if (deltaAction.has("remove")) {
            return RemoveFile.fromJsonString(deltaAction.get("remove").toString());
        } else return null;
    }

    public List<DeltaAction> readCheckpoint(List<String> checkpointFiles) throws IOException {
        List<DeltaAction> deltaActions = new ArrayList<>();
        for(String checkpointFile: checkpointFiles) {
            String checkpointFilePath = String.format("s3a://%s/%s/_delta_log/%s", bucket, path, checkpointFile);
            ParquetReader<Group> reader = ParquetReader
                    .builder(new GroupReadSupport(), new Path(checkpointFilePath))
                    .withConf(conf)
                    .build();
            Group record;
            while ((record = reader.read()) != null) {
                DeltaAction deltaAction = parseDeltaAction(record);
                if (deltaAction != null) deltaActions.add(parseDeltaAction(record));
            }
        }
        return deltaActions;
    }


    public DeltaTableSnapshot reconciliateDeltaActions(List<DeltaAction> deltaActions) {
        Map<String, AddFile> addFilesMap = new HashMap<>();
        MetaData metaData = null;
        for (DeltaAction deltaAction: deltaActions) {
            if (deltaAction instanceof AddFile) {
                AddFile addFile = (AddFile) deltaAction;
                addFilesMap.put(addFile.path, addFile);
            } else if (deltaAction instanceof RemoveFile) {
                RemoveFile removeFile = (RemoveFile) deltaAction;
                addFilesMap.remove(removeFile.path);
            } else if (deltaAction instanceof MetaData) {
                metaData = (MetaData) deltaAction;
            }
        }
        return new DeltaTableSnapshot(addFilesMap.values(), metaData);
    }

    public DeltaTableSnapshot getSnapshot() throws IOException {
        DeltaTable.LastCheckpoint lc = getLastCheckpoint();
        List<String> checkpointFiles = DeltaTable.getCheckpointFiles(lc);
        List<DeltaTable.DeltaAction> actions = readCheckpoint(checkpointFiles);
        List<String> deltaLogs = getJsonDeltaLogs(checkpointFiles.get(checkpointFiles.size() - 1));
        List<DeltaTable.DeltaAction> deltaLogsActions = readDeltaLogs(deltaLogs);
        List<DeltaTable.DeltaAction> fullActions = Stream.concat(
                actions.stream(), deltaLogsActions.stream())
                .collect(Collectors.toList());
        return reconciliateDeltaActions(fullActions);
    }


}
