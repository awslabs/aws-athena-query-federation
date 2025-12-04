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

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for extracting Parquet metadata using HTTP range requests.
 * This allows us to read only the footer of the Parquet file without downloading the entire file.
 * 
 * Based on Parquet format specification:
 * - Footer contains FileMetaData
 * - Last 8 bytes: 4-byte footer length + 4-byte magic number "PAR1"
 * - Row group information is stored in the footer
 */
public class ParquetMetadataUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetMetadataUtil.class);
    
    // Parquet constants
    private static final byte[] PARQUET_MAGIC = new byte[] { 'P', 'A', 'R', '1' };
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_LENGTH = 4;
    private static final int FOOTER_METADATA_SIZE = FOOTER_LENGTH_SIZE + MAGIC_LENGTH;
    
    // HTTP range request constants
    private static final int MAX_FOOTER_SIZE = 8 * 1024 * 1024; // 8MB max footer size
    private static final int INITIAL_FOOTER_READ_SIZE = 8192; // 8KB initial read
    private static final int CONNECTION_TIMEOUT_MS = 30000; // 30 seconds
    private static final int READ_TIMEOUT_MS = 60000; // 60 seconds
    
    /**
     * Get Parquet file metadata using HTTP range requests
     * 
     * @param presignedUrl The presigned URL to the Parquet file
     * @return ParquetFileMetadata containing row group information
     * @throws IOException if unable to read the file metadata
     */
    public static ParquetFileMetadata getParquetMetadata(String presignedUrl) throws IOException
    {
        logger.info("Getting Parquet metadata from: {}", presignedUrl.substring(0, Math.min(50, presignedUrl.length())));
        
        try {
            long fileSize = getFileSize(presignedUrl);
            logger.info("Parquet file size: {} bytes", fileSize);
            
            if (fileSize < FOOTER_METADATA_SIZE) {
                throw new IOException("File too small to be a valid Parquet file: " + fileSize);
            }
            
            byte[] footerLengthBytes = readBytes(presignedUrl, fileSize - FOOTER_METADATA_SIZE, FOOTER_METADATA_SIZE);
            
            for (int i = 0; i < MAGIC_LENGTH; i++) {
                if (footerLengthBytes[FOOTER_LENGTH_SIZE + i] != PARQUET_MAGIC[i]) {
                    throw new IOException("Not a valid Parquet file - magic number mismatch");
                }
            }
            
            int footerLength = ByteBuffer.wrap(footerLengthBytes, 0, FOOTER_LENGTH_SIZE)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
            
            logger.info("Parquet footer length: {} bytes", footerLength);
            
            if (footerLength <= 0 || footerLength > MAX_FOOTER_SIZE) {
                throw new IOException("Invalid footer length: " + footerLength);
            }
            
            long footerStart = fileSize - FOOTER_METADATA_SIZE - footerLength;
            byte[] footerBytes = readBytes(presignedUrl, footerStart, footerLength);
            
            FileMetaData fileMetaData = Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
            
            List<RowGroupMetadata> rowGroups = new ArrayList<>();
            for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
                rowGroups.add(new RowGroupMetadata(
                    rowGroup.getNum_rows(),
                    rowGroup.getTotal_byte_size(),
                    rowGroup.getTotal_compressed_size()
                ));
            }
            
            logger.info("Found {} row groups in Parquet file", rowGroups.size());
            
            return new ParquetFileMetadata(
                fileMetaData.getNum_rows(),
                rowGroups,
                fileMetaData.getCreated_by()
            );
            
        } catch (Exception e) {
            logger.error("Failed to get Parquet metadata: {}", e.getMessage(), e);
            throw new IOException("Failed to get Parquet metadata from " + presignedUrl, e);
        }
    }
    
    /**
     * Get the size of the file using HTTP HEAD request
     */
    private static long getFileSize(String presignedUrl) throws IOException
    {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(presignedUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(CONNECTION_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            connection.connect();
            
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HTTP HEAD request failed with code: " + responseCode);
            }
            
            long contentLength = connection.getContentLengthLong();
            if (contentLength < 0) {
                throw new IOException("Unable to determine file size");
            }
            
            return contentLength;
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Read bytes from the file using HTTP range request
     */
    private static byte[] readBytes(String presignedUrl, long start, int length) throws IOException
    {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(presignedUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Range", String.format("bytes=%d-%d", start, start + length - 1));
            connection.setConnectTimeout(CONNECTION_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);
            connection.connect();
            
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_PARTIAL && responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("HTTP range request failed with code: " + responseCode);
            }
            
            byte[] buffer = new byte[length];
            try (InputStream in = connection.getInputStream()) {
                int totalRead = 0;
                while (totalRead < length) {
                    int read = in.read(buffer, totalRead, length - totalRead);
                    if (read < 0) {
                        throw new IOException("Unexpected end of stream");
                    }
                    totalRead += read;
                }
            }
            
            return buffer;
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Container for Parquet file metadata
     */
    public static class ParquetFileMetadata
    {
        private final long totalRows;
        private final List<RowGroupMetadata> rowGroups;
        private final String createdBy;
        
        public ParquetFileMetadata(long totalRows, List<RowGroupMetadata> rowGroups, String createdBy)
        {
            this.totalRows = totalRows;
            this.rowGroups = rowGroups;
            this.createdBy = createdBy;
        }
        
        public long getTotalRows()
        {
            return totalRows;
        }
        
        public List<RowGroupMetadata> getRowGroups()
        {
            return rowGroups;
        }
        
        public int getRowGroupCount()
        {
            return rowGroups.size();
        }
        
        public String getCreatedBy()
        {
            return createdBy;
        }
    }
    
    /**
     * Container for row group metadata
     */
    public static class RowGroupMetadata
    {
        private final long numRows;
        private final long totalByteSize;
        private final long totalCompressedSize;
        
        public RowGroupMetadata(long numRows, long totalByteSize, long totalCompressedSize)
        {
            this.numRows = numRows;
            this.totalByteSize = totalByteSize;
            this.totalCompressedSize = totalCompressedSize;
        }
        
        public long getNumRows()
        {
            return numRows;
        }
        
        public long getTotalByteSize()
        {
            return totalByteSize;
        }
        
        public long getTotalCompressedSize()
        {
            return totalCompressedSize;
        }
    }
}
