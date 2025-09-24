/*-
 * #%L
 * athena-deltashare
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
            reader.setRequestedSchema(metadata.getFileMetaData().getSchema());
            
            for (int i = 0; i < targetRowGroupIndex; i++) {
                reader.skipNextRowGroup();
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
        int failedGroups = 0;
        
        InputFile inputFile = new PresignedRangeInputFile(signedUrl, fileSize);
        
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
            reader.setRequestedSchema(metadata.getFileMetaData().getSchema());
            
            for (int i = 0; i <= rowGroupIndex; i++) {
                if (i < rowGroupIndex) {
                    reader.skipNextRowGroup();
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
     * Custom InputFile implementation for range-based HTTP reading
     */
    static class PresignedRangeInputFile implements InputFile
    {
        private final String url;
        private final long fileSize;
        
        public PresignedRangeInputFile(String url, long fileSize)
        {
            this.url = url;
            this.fileSize = fileSize;
        }
        
        @Override
        public long getLength() throws IOException
        {
            return fileSize;
        }
        
        @Override
        public SeekableInputStream newStream() throws IOException
        {
            return new HttpRangeSeekableInputStream(url, fileSize);
        }
    }
    
    /**
     * Seekable input stream that uses HTTP range requests
     */
    static class HttpRangeSeekableInputStream extends SeekableInputStream
    {
        private final String url;
        private final long fileSize;
        private long position = 0;
        
        public HttpRangeSeekableInputStream(String url, long fileSize)
        {
            this.url = url;
            this.fileSize = fileSize;
        }
        
        @Override
        public void seek(long newPos) throws IOException
        {
            if (newPos < 0 || newPos > fileSize) {
                throw new IOException("Seek position out of bounds: " + newPos);
            }
            this.position = newPos;
        }
        
        @Override
        public long getPos() throws IOException
        {
            return position;
        }
        
        @Override
        public int read() throws IOException
        {
            byte[] b = new byte[1];
            int r = read(b, 0, 1);
            return (r == 1) ? (b[0] & 0xFF) : -1;
        }
        
        @Override
        public int read(byte[] buffer, int off, int len) throws IOException
        {
            if (position >= fileSize) {
                return -1;
            }
            
            long bytesToRead = Math.min(len, fileSize - position);
            long endPos = position + bytesToRead - 1;
            
            
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Range", "bytes=" + position + "-" + endPos);
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(60000);
            
            try {
                int responseCode = conn.getResponseCode();
                if (responseCode == 206) {
                    try (InputStream in = conn.getInputStream()) {
                        int totalRead = 0;
                        int bytesRead;
                        
                        while (totalRead < bytesToRead && 
                               (bytesRead = in.read(buffer, off + totalRead, (int)(bytesToRead - totalRead))) != -1) {
                            totalRead += bytesRead;
                        }
                        
                        position += totalRead;
                        return totalRead;
                    }
                } else {
                    throw new IOException("HTTP Range request failed with code: " + responseCode);
                }
            } finally {
                conn.disconnect();
            }
        }
        
        @Override
        public void readFully(byte[] bytes) throws IOException
        {
            readFully(bytes, 0, bytes.length);
        }
        
        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException
        {
            int totalRead = 0;
            while (totalRead < len) {
                int bytesRead = read(bytes, start + totalRead, len - totalRead);
                if (bytesRead < 0) {
                    throw new IOException("Unexpected end of stream");
                }
                totalRead += bytesRead;
            }
        }
        
        @Override
        public void readFully(java.nio.ByteBuffer buffer) throws IOException
        {
            byte[] bytes = new byte[buffer.remaining()];
            readFully(bytes);
            buffer.put(bytes);
        }
        
        @Override
        public int read(java.nio.ByteBuffer buffer) throws IOException
        {
            if (buffer.remaining() == 0) {
                return 0;
            }
            
            byte[] bytes = new byte[buffer.remaining()];
            int bytesRead = read(bytes, 0, bytes.length);
            if (bytesRead > 0) {
                buffer.put(bytes, 0, bytesRead);
            }
            return bytesRead;
        }
        
        @Override
        public long skip(long n) throws IOException
        {
            long newPos = Math.min(position + n, fileSize);
            long skipped = newPos - position;
            position = newPos;
            return skipped;
        }
        
        @Override
        public void close() throws IOException
        {
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
                    return in.readAllBytes();
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
                long fileSize = conn.getContentLengthLong();
                
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
                    try {
                        handleDateTimestamp(record, fieldIndex, block, fieldName, rowNum, null, false);
                    } catch (Exception e) {
                        logger.warn("Error processing timestamp field '{}': {}", fieldName, e.getMessage());
                        try {
                            block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
                        } catch (Exception e2) {
                            setNullValue(block, field, rowNum);
                        }
                    }
                } else if (arrowType instanceof ArrowType.Binary) {
                    Binary binary = record.getBinary(fieldIndex, 0);
                    block.setValue(fieldName, rowNum, binary.getBytes());
                } else {
                    block.setValue(fieldName, rowNum, record.getValueToString(fieldIndex, 0));
                }
                
            } catch (Exception e) {
                logger.warn("Error converting field '{}': {}", fieldName, e.getMessage());
                setNullValue(block, field, rowNum);
            }
        }
    }

    /**
     * Handles date/timestamp fields with Int96 format support.
     */
    private static void handleDateTimestamp(Group record, int fieldIndex, Block block, String fieldName, 
                                          int rowNum, ArrowType.Date dateType, boolean isDateField) throws Exception
    {
        try {
            String valueString = record.getValueToString(fieldIndex, 0);
            
            if (valueString.contains("Int96") && valueString.contains("Binary{")) {
                
                int startIdx = valueString.indexOf('[');
                int endIdx = valueString.indexOf(']');
                
                if (startIdx != -1 && endIdx != -1) {
                    String bytesStr = valueString.substring(startIdx + 1, endIdx);
                    String[] byteStrings = bytesStr.split(",\\s*");
                    
                    if (byteStrings.length == 12) {
                        byte[] int96Bytes = new byte[12];
                        for (int i = 0; i < 12; i++) {
                            int96Bytes[i] = (byte) Integer.parseInt(byteStrings[i].trim());
                        }
                        
                        long epochMillis = DateTimeConverter.convertInt96BytesToEpochMillis(int96Bytes);
                        
                        if (isDateField) {
                            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                                ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
                            } else {
                                int epochDays = (int) (epochMillis / (24 * 60 * 60 * 1000L));
                                ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
                            }
                        } else {
                            block.setValue(fieldName, rowNum, epochMillis);
                        }
                        return;
                    }
                }
                
                logger.warn("Failed to parse Int96 binary data for field '{}' - setting null", fieldName);
                if (isDateField) {
                    setNullValueForDateField(block, fieldName, rowNum, dateType);
                } else {
                    block.setValue(fieldName, rowNum, null);
                }
                return;
            }
            
            if (isDateField) {
                handleDateAsIntegerOrString(record, fieldIndex, block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, record.getLong(fieldIndex, 0));
            }
            
        } catch (ClassCastException e) {
            logger.warn("ClassCastException for field '{}': {} - setting null", fieldName, e.getMessage());
            if (isDateField) {
                setNullValueForDateField(block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        } catch (Exception e) {
            logger.warn("Error processing field '{}': {} - setting null", fieldName, e.getMessage());
            if (isDateField) {
                setNullValueForDateField(block, fieldName, rowNum, dateType);
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        }
    }

    /**
     * Sets null value for date fields.
     */
    private static void setNullValueForDateField(Block block, String fieldName, int rowNum, ArrowType.Date dateType)
    {
        try {
            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                ((DateMilliVector) block.getFieldVector(fieldName)).setNull(rowNum);
            } else {
                ((DateDayVector) block.getFieldVector(fieldName)).setNull(rowNum);
            }
        } catch (Exception e) {
            logger.warn("Failed to set null value for date field '{}': {}", fieldName, e.getMessage());
        }
    }

    /**
     * Handles dates as integer (days since epoch) or string format.
     */
    private static void handleDateAsIntegerOrString(Group record, int fieldIndex, Block block, String fieldName, 
                                                  int rowNum, ArrowType.Date dateType) throws Exception
    {
        try {
            int epochDays = record.getInteger(fieldIndex, 0);
            
            if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                long epochMillis = epochDays * 24L * 60 * 60 * 1000;
                ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
            } else {
                ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
            }
        } catch (Exception e) {
            try {
                String dateStr = record.getString(fieldIndex, 0);
                LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
                
                if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                    long epochMillis = date.atStartOfDay().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
                    ((DateMilliVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochMillis);
                } else {
                    int epochDays = (int) date.toEpochDay();
                    ((DateDayVector) block.getFieldVector(fieldName)).setSafe(rowNum, epochDays);
                }
            } catch (Exception e2) {
                logger.warn("Failed to parse date field '{}' as integer or string: {}", fieldName, e2.getMessage());
                throw e2;
            }
        }
    }

    /**
     * Sets null value for a field based on its Arrow type.
     */
    private static void setNullValue(Block block, Field field, int rowNum)
    {
        try {
            ArrowType arrowType = field.getType();
            String fieldName = field.getName();
            
            if (arrowType instanceof ArrowType.Utf8) {
                ((VarCharVector) block.getFieldVector(fieldName)).setNull(rowNum);
            } else if (arrowType instanceof ArrowType.Int) {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getBitWidth() == 32) {
                    ((IntVector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((BigIntVector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else if (arrowType instanceof ArrowType.FloatingPoint) {
                ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
                if (floatType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                    ((Float4Vector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((Float8Vector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else if (arrowType instanceof ArrowType.Date) {
                ArrowType.Date dateType = (ArrowType.Date) arrowType;
                if (dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
                    ((DateMilliVector) block.getFieldVector(fieldName)).setNull(rowNum);
                } else {
                    ((DateDayVector) block.getFieldVector(fieldName)).setNull(rowNum);
                }
            } else {
                block.setValue(fieldName, rowNum, null);
            }
        } catch (Exception e) {
            logger.warn("Failed to set null value for field '{}': {}", field.getName(), e.getMessage());
        }
    }

    @Deprecated
    public static void cleanupTempFile(String filePath)
    {
        if (filePath != null) {
            try { new File(filePath).delete(); } catch (Exception ignored) {}
        }
    }

    @Deprecated
    public static String downloadParquetFile(String signedUrl) throws IOException
    {
        throw new UnsupportedOperationException("Use streamParquetFromUrl instead");
    }

    @Deprecated
    public static void streamParquetToSpiller(String parquetFilePath, BlockSpiller spiller, Schema schema) throws IOException
    {
        throw new UnsupportedOperationException("Use streamParquetFromUrl instead");
    }
}
