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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.ArrayList;

/**
 * Utility for reading Parquet files from Delta Share URLs. Supports parallel row group processing for large files.
 */
public class ParquetReaderUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ParquetReaderUtil.class);
    private static final Configuration HADOOP_CONF = initHadoopConfig();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final long FOOTER_SIZE = 65536;
    private static final int BATCH_SIZE = 2000000;

    private static Configuration initHadoopConfig()
    {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.set("fs.file.impl.disable.cache", "true");
        return conf;
    }

    /**
     * Streams a specific row group from a Parquet file for parallel processing.
     */
    public static void streamParquetFromUrlWithRowGroup(String signedUrl, BlockSpiller spiller, Schema schema, int rowGroupIndex) throws IOException
    {
        if (signedUrl == null) {
            throw new IllegalArgumentException("Presigned URL cannot be null");
        }
        if (signedUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Presigned URL cannot be empty");
        }
        if (spiller == null) {
            throw new IllegalArgumentException("BlockSpiller cannot be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        
        try {
            long fileSize = getFileSize(signedUrl);
            
            ParquetMetadata metadata = getParquetMetadata(signedUrl, fileSize);
            List<BlockMetaData> rowGroups = metadata.getBlocks();
            
            if (rowGroupIndex >= rowGroups.size() || rowGroupIndex < 0) {
                throw new IOException("Invalid row group index " + rowGroupIndex + " for file with " + rowGroups.size() + " row groups");
            }
            
            InputFile inputFile = new PresignedRangeInputFile(signedUrl, fileSize);
            processSpecificRowGroup(inputFile, spiller, schema, metadata, rowGroupIndex);
            
        } catch (Exception e) {
            logger.error("Row group {} processing failed: {}", rowGroupIndex, e.getMessage(), e);
            throw new IOException("Row group " + rowGroupIndex + " processing failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Streams Parquet data from URL with intelligent processing based on file size.
     */
    public static void streamParquetFromUrl(String signedUrl, BlockSpiller spiller, Schema schema) throws IOException
    {
        if (signedUrl == null) {
            throw new IllegalArgumentException("Presigned URL cannot be null");
        }
        if (signedUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Presigned URL cannot be empty");
        }
        if (spiller == null) {
            throw new IllegalArgumentException("BlockSpiller cannot be null");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }
        
        try {
            long fileSize = getFileSize(signedUrl);
            
            if (fileSize <= DeltaShareConstants.LAMBDA_TEMP_LIMIT) {
                processSmallParquetFile(signedUrl, spiller, schema, fileSize);
            } else {
                ParquetMetadata metadata = getParquetMetadata(signedUrl, fileSize);
                processRowGroupsViaRangeRequests(signedUrl, spiller, schema, metadata, fileSize);
            }
            
        } catch (Exception e) {
            logger.error("Parquet processing failed: {}", e.getMessage(), e);
            throw new IOException("Parquet processing failed: " + e.getMessage(), e);
        }
    }

    /**
     * Gets Parquet metadata by reading the file footer using Range requests.
     */
    public static ParquetMetadata getParquetMetadata(String signedUrl, long fileSize) throws IOException
    {
        long footerStart = Math.max(0, fileSize - FOOTER_SIZE);
        byte[] footerBytes = fetchByteRange(signedUrl, footerStart, fileSize - 1);
        
        String tempFooterFile = "/tmp/metadata_" + System.currentTimeMillis() + ".parquet";
        try {
            try (java.io.FileOutputStream out = new java.io.FileOutputStream(tempFooterFile)) {
                out.write(footerBytes);
            }
            
            Path footerPath = new Path("file://" + tempFooterFile);
            try (ParquetFileReader reader = ParquetFileReader.open(HADOOP_CONF, footerPath)) {
                return reader.getFooter();
            }
            
        } finally {
            new File(tempFooterFile).delete();
        }
    }

    /**
     * Processes smaller Parquet files that fit in Lambda temp storage.
     */
    private static void processSmallParquetFile(String signedUrl, BlockSpiller spiller, Schema schema, long fileSize) 
        throws IOException
    {
        String tempFile = "/tmp/deltashare_" + System.currentTimeMillis() + ".parquet";
        
        try {
            try (InputStream in = new URL(signedUrl).openStream();
                 java.io.FileOutputStream out = new java.io.FileOutputStream(tempFile)) {
                
                byte[] buffer = new byte[1024 * 1024];
                int bytesRead;
                long totalBytes = 0;
                
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    totalBytes += bytesRead;
                    
                    if (totalBytes > DeltaShareConstants.LAMBDA_TEMP_LIMIT) {
                        throw new IOException("File size exceeds Lambda temp storage limit");
                    }
                }
            }
            
            processCompleteParquetFile(tempFile, spiller, schema, fileSize);
            
        } finally {
            new File(tempFile).delete();
        }
    }

    /**
     * Processes complete Parquet file with batch streaming.
     */
    private static void processCompleteParquetFile(String filePath, BlockSpiller spiller, Schema schema, long fileSize) 
        throws IOException
    {
        try {
            Path path = new Path("file://" + filePath);
            
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(HADOOP_CONF)
                    .build()) {
                
                Group record;
                List<Group> batchRecords = new ArrayList<>(BATCH_SIZE);
                int batchRowCount = 0;
                
                while ((record = reader.read()) != null) {
                    batchRecords.add(record);
                    batchRowCount++;
                    
                    if (batchRowCount >= BATCH_SIZE) {
                        for (int chunkStart = 0; chunkStart < batchRecords.size(); chunkStart += 100) {
                            final int finalChunkStart = chunkStart;
                            final int chunkEnd = Math.min(chunkStart + 100, batchRecords.size());
                            final int chunkSize = chunkEnd - chunkStart;
                            
                            spiller.writeRows((Block block, int startRowNum) -> {
                                for (int i = finalChunkStart; i < chunkEnd; i++) {
                                    writeParquetRecordToBlock(block, startRowNum + (i - finalChunkStart), batchRecords.get(i), schema);
                                }
                                return chunkSize;
                            });
                        }
                        
                        batchRecords.clear();
                        batchRowCount = 0;
                        
                        if (batchRowCount % (BATCH_SIZE * 50) == 0) {
                            System.gc();
                        }
                    }
                }
                
                if (!batchRecords.isEmpty()) {
                    for (int chunkStart = 0; chunkStart < batchRecords.size(); chunkStart += 100) {
                        final int finalChunkStart = chunkStart;
                        final int chunkEnd = Math.min(chunkStart + 100, batchRecords.size());
                        final int chunkSize = chunkEnd - chunkStart;
                        
                        spiller.writeRows((Block block, int startRowNum) -> {
                            for (int i = finalChunkStart; i < chunkEnd; i++) {
                                writeParquetRecordToBlock(block, startRowNum + (i - finalChunkStart), batchRecords.get(i), schema);
                            }
                            return chunkSize;
                        });
                    }
                    
                    batchRecords.clear();
                }
            }
        } catch (Exception e) {
            logger.error("Failed to process complete Parquet file: {}", e.getMessage(), e);
            throw new IOException("Complete Parquet processing failed: " + e.getMessage(), e);
        }
    }

    /**
     * Processes a specific row group from a Parquet file for parallel execution.
     */
    private static void processSpecificRowGroup(InputFile inputFile, BlockSpiller spiller, Schema schema,
                                              ParquetMetadata metadata, int targetRowGroupIndex) throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            for (int i = 0; i < targetRowGroupIndex; i++) {
                reader.readNextRowGroup();
            }
            
            org.apache.parquet.column.page.PageReadStore pages = reader.readNextRowGroup();
            
            if (pages != null) {
                long rowsInGroup = pages.getRowCount();
                
                org.apache.parquet.io.MessageColumnIO columnIO =
                    new org.apache.parquet.io.ColumnIOFactory().getColumnIO(metadata.getFileMetaData().getSchema());
                
                RecordMaterializer<Group> recordMaterializer = new GroupRecordConverter(metadata.getFileMetaData().getSchema());
                org.apache.parquet.io.RecordReader<Group> recordReader = 
                    columnIO.getRecordReader(pages, recordMaterializer);
                
                List<Group> batchRecords = new ArrayList<>();
                
                for (long row = 0; row < rowsInGroup; row++) {
                    final Group record = recordReader.read();
                    if (record != null) {
                        batchRecords.add(record);
                        
                        if (batchRecords.size() >= 100 || row == rowsInGroup - 1) {
                            final int batchSize = batchRecords.size();
                            
                            for (int chunkStart = 0; chunkStart < batchSize; chunkStart += 100) {
                                final int finalChunkStart = chunkStart;
                                final int chunkEnd = Math.min(chunkStart + 100, batchSize);
                                final int chunkSize = chunkEnd - chunkStart;
                                
                                spiller.writeRows((Block block, int startRowNum) -> {
                                    for (int i = finalChunkStart; i < chunkEnd; i++) {
                                        writeParquetRecordToBlock(block, startRowNum + (i - finalChunkStart), batchRecords.get(i), schema);
                                    }
                                    return chunkSize;
                                });
                            }
                            
                            batchRecords.clear();
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Processes all row groups in a large Parquet file using range requests.
     */
    private static void processRowGroupsViaRangeRequests(String signedUrl, BlockSpiller spiller, Schema schema, 
                                                       ParquetMetadata metadata, long fileSize) throws IOException
    {
        List<BlockMetaData> rowGroups = metadata.getBlocks();
        InputFile inputFile = new PresignedRangeInputFile(signedUrl, fileSize);
        
        int failedGroups = 0;
        for (int groupIndex = 0; groupIndex < rowGroups.size(); groupIndex++) {
            try {
                processRowGroup(inputFile, spiller, schema, metadata, groupIndex);
            } catch (Exception e) {
                logger.error("Failed to process row group {}: {}", groupIndex, e.getMessage(), e);
                failedGroups++;
            }
        }
        
        if (failedGroups > 0) {
            logger.error("Failed to process {} row groups", failedGroups);
        }
    }
    
    /**
     * Process a single row group from the Parquet file
     */
    private static void processRowGroup(InputFile inputFile, BlockSpiller spiller, Schema schema,
                                      ParquetMetadata metadata, int rowGroupIndex) throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            for (int i = 0; i <= rowGroupIndex; i++) {
                if (i < rowGroupIndex) {
                    reader.readNextRowGroup();
                } else {
                    org.apache.parquet.column.page.PageReadStore pages = reader.readNextRowGroup();
                    
                    if (pages != null) {
                        long rowsInGroup = pages.getRowCount();
                        
                        org.apache.parquet.io.MessageColumnIO columnIO = 
                            new org.apache.parquet.io.ColumnIOFactory().getColumnIO(metadata.getFileMetaData().getSchema());
                        
                        RecordMaterializer<Group> recordMaterializer = new GroupRecordConverter(metadata.getFileMetaData().getSchema());
                        org.apache.parquet.io.RecordReader<Group> recordReader = 
                            columnIO.getRecordReader(pages, recordMaterializer);
                        
                        for (long row = 0; row < rowsInGroup; row++) {
                            final Group record = recordReader.read();
                            if (record != null) {
                                spiller.writeRows((Block block, int rowNum) -> {
                                    writeParquetRecordToBlock(block, rowNum, record, schema);
                                    return 1;
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Fetch byte range using HTTP Range request
     */
    private static byte[] fetchByteRange(String signedUrl, long startByte, long endByte) throws IOException
    {
        HttpURLConnection conn = (HttpURLConnection) new URL(signedUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Range", "bytes=" + startByte + "-" + endByte);
        conn.setConnectTimeout(15000);
        conn.setReadTimeout(60000);
        
        try {
            int responseCode = conn.getResponseCode();
            if (responseCode == 206) {
                try (InputStream in = conn.getInputStream()) {
                    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
                    byte[] data = new byte[1024];
                    int nRead;
                    while ((nRead = in.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    return buffer.toByteArray();
                }
            } else {
                throw new IOException("HTTP Range request failed with code: " + responseCode);
            }
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Get file size using HEAD request with Range fallback
     */
    private static long getFileSize(String signedUrl) throws IOException
    {
        HttpURLConnection conn = (HttpURLConnection) new URL(signedUrl).openConnection();
        conn.setRequestMethod("HEAD");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(30000);
        
        try {
            int responseCode = conn.getResponseCode();
            
            if (responseCode == 200) {
                long fileSize = conn.getContentLength();
                
                if (fileSize > 0) {
                    return fileSize;
                }
            }
        } catch (Exception e) {
            logger.warn("HEAD request failed, falling back to Range request: {}", e.getMessage());
        } finally {
            conn.disconnect();
        }
        
        conn = (HttpURLConnection) new URL(signedUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Range", "bytes=0-0");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(30000);
        
        try {
            int responseCode = conn.getResponseCode();
            
            if (responseCode == 206) {
                String contentRange = conn.getHeaderField("Content-Range");
                
                if (contentRange != null && contentRange.contains("/")) {
                    String[] parts = contentRange.split("/");
                    
                    if (parts.length >= 2) {
                        long fileSize = Long.parseLong(parts[1]);
                        return fileSize;
                    }
                }
            }
            
            throw new IOException("Unable to determine file size");
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Converts Parquet Group record to Arrow Block format.
     */
    private static void writeParquetRecordToBlock(Block block, int rowNum, Group record, Schema schema)
    {
        writeParquetRecordToBlock(block, rowNum, record, schema, null);
    }
    
    /**
     * Converts Parquet Group record to Arrow Block format with partition value injection.
     */
    private static void writeParquetRecordToBlock(Block block, int rowNum, Group record, Schema schema, java.util.Map<String, String> partitionValues)
    {
        GroupType parquetSchema = record.getType();
        
        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            
            try {
                if (!parquetSchema.containsField(fieldName)) {
                    if (partitionValues != null && partitionValues.containsKey(fieldName)) {
                        String partitionValue = partitionValues.get(fieldName);
                        if (partitionValue != null) {
                            block.setValue(fieldName, rowNum, partitionValue);
                            continue;
                        }
                    }
                    setNullValue(block, field, rowNum);
                    continue;
                }
                
                int fieldIndex = parquetSchema.getFieldIndex(fieldName);
                if (record.getFieldRepetitionCount(fieldIndex) == 0) {
                    setNullValue(block, field, rowNum);
                    continue;
                }
                
                ArrowType arrowType = field.getType();
                
                if (arrowType instanceof ArrowType.Utf8) {
                    block.setValue(fieldName, rowNum, record.getString(fieldIndex, 0));
                } else if (arrowType instanceof ArrowType.Int) {
                    ArrowType.Int intType = (ArrowType.Int) arrowType;
                    if (intType.getBitWidth() == 32) {
                        block.setValue(fieldName, rowNum, record.getInteger(fieldIndex, 0));
                    } else {
                        block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
                    }
                } else if (arrowType instanceof ArrowType.FloatingPoint) {
                    ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                    if (floatType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                        block.setValue(fieldName, rowNum, record.getFloat(fieldIndex, 0));
                    } else {
                        block.setValue(fieldName, rowNum, record.getDouble(fieldIndex, 0));
                    }
                } else if (arrowType instanceof ArrowType.Bool) {
                    block.setValue(fieldName, rowNum, record.getBoolean(fieldIndex, 0));
                } else if (arrowType instanceof ArrowType.Date) {
                    ArrowType.Date dateType = (ArrowType.Date) arrowType;
                    
                    try {
                        handleDateTimestamp(record, fieldIndex, block, fieldName, rowNum, dateType, true);
                    } catch (Exception e) {
                        logger.warn("Error processing date field '{}': {}", fieldName, e.getMessage());
                        setNullValue(block, field, rowNum);
                    }
                } else if (arrowType instanceof ArrowType.Timestamp) {
                    ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                    
                    try {
                        handleDateTimestamp(record, fieldIndex, block, fieldName, rowNum, timestampType, false);
                    } catch (Exception e) {
                        logger.warn("Error processing timestamp field '{}': {}", fieldName, e.getMessage());
                        setNullValue(block, field, rowNum);
                    }
                } else {
                    logger.warn("Unsupported Arrow type: {} for field: {}", arrowType, fieldName);
                    setNullValue(block, field, rowNum);
                }
                
            } catch (Exception e) {
                logger.warn("Error processing field '{}': {}", fieldName, e.getMessage());
                setNullValue(block, field, rowNum);
            }
        }
    }

    /**
     * Handles date and timestamp field conversion from Parquet to Arrow format.
     */
    private static void handleDateTimestamp(Group record, int fieldIndex, Block block, String fieldName, 
                                          int rowNum, ArrowType type, boolean isDate) throws Exception
    {
        try {
            if (isDate) {
                ArrowType.Date dateType = (ArrowType.Date) type;
                
                if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.DAY) {
                    int daysSinceEpoch = record.getInteger(fieldIndex, 0);
                    block.setValue(fieldName, rowNum, daysSinceEpoch);
                } else {
                    long millisSinceEpoch = record.getLong(fieldIndex, 0);
                    block.setValue(fieldName, rowNum, millisSinceEpoch);
                }
            } else {
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) type;
                
                Binary timestampBinary = record.getBinary(fieldIndex, 0);
                long timestampValue = DateTimeConverter.convertInt96BytesToEpochMillis(timestampBinary.getBytes());
                
                if (timestampType.getUnit() == org.apache.arrow.vector.types.TimeUnit.MILLISECOND) {
                    block.setValue(fieldName, rowNum, timestampValue);
                } else if (timestampType.getUnit() == org.apache.arrow.vector.types.TimeUnit.MICROSECOND) {
                    block.setValue(fieldName, rowNum, timestampValue * 1000);
                } else if (timestampType.getUnit() == org.apache.arrow.vector.types.TimeUnit.NANOSECOND) {
                    block.setValue(fieldName, rowNum, timestampValue * 1000000);
                } else {
                    block.setValue(fieldName, rowNum, timestampValue / 1000);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to convert date/timestamp for field '{}': {}", fieldName, e.getMessage());
            throw e;
        }
    }

    /**
     * Sets null value for a field in the block based on Arrow type.
     */
    private static void setNullValue(Block block, Field field, int rowNum)
    {
        try {
            ArrowType arrowType = field.getType();
            
            if (arrowType instanceof ArrowType.Utf8) {
                block.setValue(field.getName(), rowNum, null);
            } else if (arrowType instanceof ArrowType.Int) {
                block.setValue(field.getName(), rowNum, null);
            } else if (arrowType instanceof ArrowType.FloatingPoint) {
                block.setValue(field.getName(), rowNum, null);
            } else if (arrowType instanceof ArrowType.Bool) {
                block.setValue(field.getName(), rowNum, null);
            } else if (arrowType instanceof ArrowType.Date) {
                block.setValue(field.getName(), rowNum, null);
            } else if (arrowType instanceof ArrowType.Timestamp) {
                block.setValue(field.getName(), rowNum, null);
            } else {
                block.setValue(field.getName(), rowNum, null);
            }
        } catch (Exception e) {
            logger.warn("Failed to set null value for field '{}': {}", field.getName(), e.getMessage());
        }
    }
}