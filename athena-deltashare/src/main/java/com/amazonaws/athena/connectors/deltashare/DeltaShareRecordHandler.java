/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.deltashare.client.DeltaShareClient;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Handles data records for Delta Share. Supports parallel row group processing and partition value injection.
 */
public class DeltaShareRecordHandler extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaShareRecordHandler.class);
    
    private static final String PROCESSING_MODE_METADATA = "processing_mode";
    private static final String PRESIGNED_URL_METADATA = "presigned_url";
    private static final String ROW_GROUP_INDEX_METADATA = "row_group_index";
    private static final String TOTAL_ROW_GROUPS_METADATA = "total_row_groups";
    private static final String FILE_SIZE_METADATA = "file_size";
    
    private final DeltaShareClient deltaShareClient;
    private final ObjectMapper objectMapper;
    private final String configuredShareName;

    public DeltaShareRecordHandler(Map<String, String> configOptions)
    {
        super(DeltaShareConstants.SOURCE_TYPE, configOptions);
        String endpoint = configOptions.get(DeltaShareConstants.ENDPOINT_PROPERTY);
        String token = configOptions.get(DeltaShareConstants.TOKEN_PROPERTY);
        this.configuredShareName = configOptions.get(DeltaShareConstants.SHARE_NAME_PROPERTY);
        this.deltaShareClient = new DeltaShareClient(endpoint, token);
        this.objectMapper = new ObjectMapper();
        
        if (configuredShareName == null || configuredShareName.isEmpty()) {
            throw new RuntimeException("share_name must be configured in environment variables");
        }
    }
    
    /**
     * Configures S3 spill with parallel upload threads for performance.
     */
    @Override
    protected SpillConfig getSpillConfig(ReadRecordsRequest request)
    {
        long maxBlockSize = request.getMaxBlockSize();
        if (configOptions.get("MAX_BLOCK_SIZE_BYTES") != null) {
            maxBlockSize = Long.parseLong(configOptions.get("MAX_BLOCK_SIZE_BYTES"));
        }
        
        return SpillConfig.newBuilder()
                .withSpillLocation(request.getSplit().getSpillLocation())
                .withMaxBlockBytes(maxBlockSize)
                .withMaxInlineBlockBytes(request.getMaxInlineBlockSize())
                .withRequestId(request.getQueryId())
                .withEncryptionKey(request.getSplit().getEncryptionKey())
                .withNumSpillThreads(8)
                .build();
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        Map<String, String> splitMetadata = recordsRequest.getSplit().getProperties();
        String processingMode = splitMetadata.getOrDefault(PROCESSING_MODE_METADATA, "STANDARD");
        
        java.util.Map<String, String> partitionValues = extractPartitionValues(splitMetadata, recordsRequest.getSchema());
        
        try {
            switch (processingMode) {
                case "ROW_GROUP":
                case "SINGLE_FILE":
                    handleParallelRowGroupProcessing(spiller, recordsRequest, queryStatusChecker, partitionValues);
                    break;
                case "STANDARD":
                default:
                    handleStandardProcessing(spiller, recordsRequest, queryStatusChecker, partitionValues);
                    break;
            }
        } catch (Exception e) {
            logger.error("Failed to process Delta Share data: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process Delta Share data", e);
        }
    }
    
    /**
     * Extracts partition column values from split metadata for injection into result data.
     */
    private java.util.Map<String, String> extractPartitionValues(Map<String, String> splitMetadata, Schema schema)
    {
        java.util.Map<String, String> partitionValues = new java.util.HashMap<>();
        
        String partitionColsMetadata = schema.getCustomMetadata().get("partitionCols");
        if (partitionColsMetadata == null || partitionColsMetadata.isEmpty()) {
            return partitionValues;
        }
        
        String[] partitionColumns = partitionColsMetadata.split(",");
        
        for (String partitionCol : partitionColumns) {
            String cleanPartitionCol = partitionCol.trim();
            String partitionValue = splitMetadata.get(cleanPartitionCol);
            
            if (partitionValue != null) {
                partitionValues.put(cleanPartitionCol, partitionValue);
            }
        }
        
        return partitionValues;
    }
    
    /**
     * Handles parallel row group processing for large files. Each Lambda processes only its assigned row group.
     */
    private void handleParallelRowGroupProcessing(BlockSpiller spiller, ReadRecordsRequest recordsRequest, 
                                                  QueryStatusChecker queryStatusChecker, java.util.Map<String, String> partitionValues) throws Exception
    {
        Map<String, String> splitMetadata = recordsRequest.getSplit().getProperties();
        String presignedUrl = splitMetadata.get(PRESIGNED_URL_METADATA);
        
        if (presignedUrl == null) {
            throw new RuntimeException("No presigned URL found in split metadata");
        }
        
        String processingMode = splitMetadata.getOrDefault(PROCESSING_MODE_METADATA, "STANDARD");
        int rowGroupIndex = Integer.parseInt(splitMetadata.getOrDefault(ROW_GROUP_INDEX_METADATA, "-1"));
        int totalRowGroups = Integer.parseInt(splitMetadata.getOrDefault(TOTAL_ROW_GROUPS_METADATA, "1"));
        long fileSize = Long.parseLong(splitMetadata.getOrDefault(FILE_SIZE_METADATA, "0"));
        
        Schema schema = recordsRequest.getSchema();
        
        if ("ROW_GROUP".equals(processingMode) && rowGroupIndex >= 0) {
            com.amazonaws.athena.connectors.deltashare.util.ParquetReaderUtil.streamParquetFromUrlWithRowGroup(presignedUrl, spiller, schema, rowGroupIndex);
        } else {
            com.amazonaws.athena.connectors.deltashare.util.ParquetReaderUtil.streamParquetFromUrl(presignedUrl, spiller, schema);
        }
    }
    
    /**
     * Handles standard processing for smaller files and fallback scenarios.
     */
    private void handleStandardProcessing(BlockSpiller spiller, ReadRecordsRequest recordsRequest, 
                                         QueryStatusChecker queryStatusChecker, java.util.Map<String, String> partitionValues) throws Exception
    {
        String schemaName = recordsRequest.getTableName().getSchemaName();
        String tableName = recordsRequest.getTableName().getTableName();
        String partitionId = recordsRequest.getSplit().getProperty("partition_id");
        
        JsonNode queryResponse = deltaShareClient.queryTable(configuredShareName, schemaName, tableName);
        
        if (queryResponse == null || !queryResponse.isArray()) {
            logger.warn("No valid response from Delta Share");
            return;
        }
        
        int failedFiles = 0;
        
        for (JsonNode line : queryResponse) {
            if (line.has("file")) {
                JsonNode file = line.get("file");
                String filePartitionKey = getPartitionKey(file);
                
                if (matchesPartition(filePartitionKey, partitionId)) {
                    if (!file.has("url")) {
                        logger.warn("File entry missing URL, skipping file");
                        failedFiles++;
                        continue;
                    }
                    
                    String signedUrl = file.get("url").asText();
                    if (signedUrl == null || signedUrl.trim().isEmpty()) {
                        logger.warn("File entry has empty URL, skipping file");
                        failedFiles++;
                        continue;
                    }
                    
                    try {
                        com.amazonaws.athena.connectors.deltashare.util.ParquetReaderUtil.streamParquetFromUrl(signedUrl, spiller, recordsRequest.getSchema());
                        
                        if (!partitionValues.isEmpty()) {
                            spiller.writeRows((Block block, int startRow) -> {
                                int blockRowCount = block.getRowCount();
                                for (int row = 0; row < blockRowCount; row++) {
                                    for (java.util.Map.Entry<String, String> entry : partitionValues.entrySet()) {
                                        try {
                                            block.setValue(entry.getKey(), row, entry.getValue());
                                        } catch (Exception e) {
                                            logger.warn("Failed to inject partition value: {}", e.getMessage());
                                        }
                                    }
                                }
                                return 0;
                            });
                        }
                        
                    } catch (Exception e) {
                        logger.warn("Parquet processing failed for file, skipping: {}", e.getMessage());
                    }
                }
            }
        }
    }
    
    
    /**
     * Gets partition key from file metadata.
     */
    private String getPartitionKey(JsonNode file)
    {
        if (file.has("partitionValues")) {
            JsonNode partitionValues = file.get("partitionValues");
            if (partitionValues.size() == 0) {
                return "no_partition";
            }
            
            StringBuilder keyBuilder = new StringBuilder();
            for (Iterator<String> fieldNames = partitionValues.fieldNames(); fieldNames.hasNext();) {
                String fieldName = fieldNames.next();
                JsonNode value = partitionValues.get(fieldName);
                if (keyBuilder.length() > 0) {
                    keyBuilder.append(",");
                }
                keyBuilder.append(fieldName).append("=").append(value.asText());
            }
            return keyBuilder.toString();
        }
        return "no_partition";
    }
    
    /**
     * Checks if file partition matches split partition.
     */
    private boolean matchesPartition(String filePartitionKey, String splitPartitionId)
    {
        if (splitPartitionId == null || "default".equals(splitPartitionId) || "no_partition".equals(splitPartitionId)) {
            return "no_partition".equals(filePartitionKey);
        }
        return splitPartitionId.equals(filePartitionKey);
    }
}
