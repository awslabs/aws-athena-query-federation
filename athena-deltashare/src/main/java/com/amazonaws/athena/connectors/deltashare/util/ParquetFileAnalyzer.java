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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Analyzes Parquet files to determine processing strategy
 * Extracted from DeltaShareMetadataHandler for better maintainability
 */
public class ParquetFileAnalyzer 
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetFileAnalyzer.class);
    

    /**
     * Result of file analysis
     */
    public static class FileAnalysisResult 
    {
        public final boolean requiresRowGroupPartitioning;
        public final boolean canProcessInLambda;
        public final int actualRowGroupCount;
        public final long totalRows;
        public final long fileSizeBytes;
        public final String processingStrategy;
        
        public FileAnalysisResult(boolean requiresRowGroupPartitioning, boolean canProcessInLambda,
                                 int actualRowGroupCount, long totalRows, long fileSizeBytes, 
                                 String processingStrategy) 
        {
            this.requiresRowGroupPartitioning = requiresRowGroupPartitioning;
            this.canProcessInLambda = canProcessInLambda;
            this.actualRowGroupCount = actualRowGroupCount;
            this.totalRows = totalRows;
            this.fileSizeBytes = fileSizeBytes;
            this.processingStrategy = processingStrategy;
        }
    }

    /**
     * Analyze file to determine best processing strategy
     */
    public static FileAnalysisResult analyzeFile(String presignedUrl, long fileSize) throws IOException
    {
        try {
            ParquetMetadata metadata = ParquetMetadataReader.getParquetMetadata(presignedUrl, fileSize);
            List<BlockMetaData> rowGroups = metadata.getBlocks();
            
            int actualRowGroupCount = rowGroups.size();
            long totalRows = rowGroups.stream().mapToLong(BlockMetaData::getRowCount).sum();
            
            boolean canProcessInLambda = fileSize <= DeltaShareConstants.LAMBDA_TEMP_LIMIT;
            boolean requiresRowGroupPartitioning = shouldPartitionRowGroups(fileSize, rowGroups);
            
            String processingStrategy = determineProcessingStrategy(fileSize, actualRowGroupCount, canProcessInLambda, requiresRowGroupPartitioning);
            
            return new FileAnalysisResult(requiresRowGroupPartitioning, canProcessInLambda, 
                                        actualRowGroupCount, totalRows, fileSize, processingStrategy);
            
        } catch (IOException e) {
            logger.error("File analysis failed: {}", e.getMessage(), e);
            throw new IOException("Failed to analyze Parquet file: " + e.getMessage(), e);
        }
    }

    /**
     * Check if file requires row group partitioning for Lambda processing
     */
    public static boolean requiresRowGroupPartitioning(long fileSize)
    {
        return fileSize > DeltaShareConstants.FILE_SIZE_THRESHOLD;
    }

    /**
     * Get actual row group count from metadata
     */
    public static int getActualRowGroupCount(String presignedUrl, long fileSize) throws IOException
    {
        ParquetMetadata metadata = ParquetMetadataReader.getParquetMetadata(presignedUrl, fileSize);
        return metadata.getBlocks().size();
    }

    /**
     * Determine if we should partition row groups based on file characteristics
     */
    private static boolean shouldPartitionRowGroups(long fileSize, List<BlockMetaData> rowGroups)
    {
        if (fileSize > DeltaShareConstants.LAMBDA_TEMP_LIMIT) {
            return true;
        }
        
        long largeRowGroupCount = rowGroups.stream()
            .mapToLong(BlockMetaData::getTotalByteSize)
            .filter(size -> size > DeltaShareConstants.ROW_GROUP_SIZE_THRESHOLD)
            .count();
        
        if (largeRowGroupCount > 5) {
            return true;
        }
        
        return false;
    }

    /**
     * Determine the best processing strategy for this file
     */
    private static String determineProcessingStrategy(long fileSize, int rowGroupCount, 
                                                     boolean canProcessInLambda, boolean requiresRowGroupPartitioning)
    {
        if (!canProcessInLambda) {
            if (requiresRowGroupPartitioning) {
                return "HTTP_RANGE_STREAMING_WITH_ROW_GROUP_PARTITIONING";
            } else {
                return "HTTP_RANGE_STREAMING_FULL_FILE";
            }
        } else {
            if (requiresRowGroupPartitioning) {
                return "LAMBDA_TEMP_WITH_ROW_GROUP_PARTITIONING";
            } else {
                return "LAMBDA_TEMP_FULL_FILE";
            }
        }
    }
}
