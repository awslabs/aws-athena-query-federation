/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Reads Parquet file metadata without downloading the entire file to Lambda storage.
 */
public class ParquetMetadataReader 
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetMetadataReader.class);

    /**
     * Read Parquet metadata using HTTP range requests
     * This avoids downloading the entire file to /tmp
     */
    public static ParquetMetadata getParquetMetadata(String signedUrl, long fileSize) throws IOException
    {
        logger.info("Reading Parquet metadata from URL: {} (size: {} bytes)",
                    signedUrl.substring(0, Math.min(100, signedUrl.length())), fileSize);
        
        long startTime = System.currentTimeMillis();
        
        try {
            InputFile inputFile = new PresignedRangeInputFile(signedUrl, fileSize);
            
            try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                ParquetMetadata metadata = reader.getFooter();
                
                long readTime = System.currentTimeMillis() - startTime;
                logger.info("Successfully read Parquet metadata in {}ms. Row groups: {}, Columns: {}", 
                           readTime, 
                           metadata.getBlocks().size(),
                           metadata.getFileMetaData().getSchema().getFieldCount());
                
                return metadata;
            }
            
        } catch (IOException e) {
            logger.error("Failed to read Parquet metadata from URL: {}", 
                        signedUrl.substring(0, Math.min(100, signedUrl.length())), e);
            throw new IOException("Failed to read Parquet metadata: " + e.getMessage(), e);
        }
    }
}
