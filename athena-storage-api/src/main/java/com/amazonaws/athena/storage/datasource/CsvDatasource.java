/*-
 * #%L
 * athena-hive
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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.athena.storage.datasource;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.StorageUtil;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.amazonaws.athena.storage.common.StorageObject;
import com.amazonaws.athena.storage.common.StorageObjectField;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.datasource.csv.ConstraintEvaluator;
import com.amazonaws.athena.storage.datasource.csv.CsvFilter;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.datasource.exception.LoadSchemaFailedException;
import com.amazonaws.athena.storage.datasource.exception.ReadRecordsException;
import com.amazonaws.athena.storage.datasource.exception.TableNotFoundException;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.amazonaws.athena.storage.gcs.GcsCsvSplitUtil;
import com.amazonaws.athena.storage.gcs.GroupSplit;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.google.common.collect.ImmutableList;
import com.univocity.parsers.common.record.RecordMetaData;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.storage.StorageConstants.MAX_CSV_FILES_SIZE;
import static com.amazonaws.athena.storage.StorageConstants.STORAGE_SPLIT_JSON;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityName;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CsvDatasource
        extends AbstractStorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvDatasource.class);

    // Used by reflection
    @SuppressWarnings("unused")
    public CsvDatasource(String storageCredentialJsonString,
                         Map<String, String> properties) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this(new StorageDatasourceConfig()
                .credentialsJson(storageCredentialJsonString)
                .properties(properties));
    }

    public CsvDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        super(config);
    }

    /**
     * Indicates whether a ths datasource supports grouping of multiple files to form a single table
     *
     * @return This datasource doesn't support reading multiple file to form a single table. So it always returns false
     */
    @Override
    public boolean supportsPartitioning()
    {
        return true;
    }

    @Override
    public List<FilterExpression> getAllFilterExpressions(Constraints constraints, String bucketName, String objectName)
    {
        return List.of();
    }

    @Override
    public boolean isExtensionCheckMandatory()
    {
        return true;
    }

    @Override
    public StorageObjectSchema getObjectSchema(String bucket, String objectName) throws IOException
    {
        try (InputStream inputStream = storageProvider.getOnlineInputStream(bucket, objectName)) {
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            CsvParserSettings settings = new CsvParserSettings();
            settings.setHeaderExtractionEnabled(true); // grabs headers from input
            settings.setMaxCharsPerColumn(10240);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(reader);
            String[] headers = parser.getRecordMetadata().headers();
            List<StorageObjectField> fieldList = new ArrayList<>();
            for (int i = 0; i < headers.length; i++) {
                fieldList.add(StorageObjectField.builder()
                        .columnName(headers[i])
                        .columnIndex(i)
                        .build());
            }
            return StorageObjectSchema.builder()
                    .fields(fieldList)
                    .baseSchema(headers)
                    .build();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FilterExpression> getExpressions(String bucket, String objectName, Schema schema, TableName tableName, Constraints constraints,
                                                 Map<String, String> partitionFieldValueMap)
    {
        return new CsvFilter()
                .evaluator(schema, constraints, tableName, partitionFieldValueMap)
                .getExpressions();
    }

    @Override
    public boolean isSupported(String bucket, String objectName)
    {
        return objectName.toLowerCase().endsWith(datasourceConfig.extension());
    }

    @Override
    public Optional<String> getBaseName(String bucket, String objectName)
    {
        if (storageProvider.isDirectory(bucket, objectName)) {
            // TODO: recurse to find base file
            System.out.println(objectName + " is a directory");
        }
        else if (isSupported(bucket, objectName)) {
            return Optional.of(objectName);
        }
        return Optional.empty();
    }

    @Override
    public List<StorageSplit> getSplitsByStoragePartition(StoragePartition partition, boolean partitioned, String partitionBase)
    {
        List<String> fileNames;
        if (partitioned) {
            LOGGER.info("Location {} is a directory, walking through", partition.getLocation());
            fileNames = storageProvider.getLeafObjectsByPartitionPrefix(partition.getBucketName(), partitionBase, 0);
        }
        else {
            fileNames = List.of(partition.getLocation());
        }
        LOGGER.info("Splitting based on file list: {}", fileNames);
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            checkFilesSize(partition.getBucketName(), fileName);
            InputStream inputStream;
            try {
                inputStream = storageProvider.getOfflineInputStream(partition.getBucketName(), fileName);
                long totalRecords = StorageUtil.getCsvRecordCount(inputStream);
                LOGGER.info("Total record found in file {} was {}", fileName, totalRecords);
                splits.addAll(GcsCsvSplitUtil.getStorageSplitList(totalRecords, fileName, recordsPerSplit()));
            }
            catch (IOException exception) {
                // the file might not be supported or corrupted
                // ignored, but logged
                LOGGER.error("Unable to read Splits from the file {}, under the bucket {} due to error: {}", fileName,
                        partition.getBucketName(), exception.getMessage(), exception);
            }
        }
        StorageUtil.printJson(splits, "Csv Splits");
        return splits;
    }

    /**
     * Returns splits, usually by page size with offset and limit so that lambda can parallelize to load data against a given SQL statement
     *
     * @param schema      Schema of the table
     * @param constraints Constraint if any
     * @param tableInfo   Table info with table and schema name
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file under the bucket
     * @return An instance of {@link StorageSplit}
     */
    @Override
    public List<StorageSplit> getStorageSplits(Schema schema, Constraints constraints, TableName tableInfo,
                                               String bucketName, String objectNames) throws IOException
    {
        checkFilesSize(bucketName, objectNames);
        return getStorageSplits(bucketName, objectNames);
    }

    /**
     * Returns splits, usually by page size with offset and limit so that lambda can parallelize to load data against a given SQL statement
     *
     * @param bucketName Name of the bucket
     * @param fileName   Name of the file under the bucket
     * @return An instance of {@link StorageSplit}
     */
    @Override
    public List<StorageSplit> getStorageSplits(final String bucketName,
                                               final String fileName) throws IOException
    {
        InputStream inputStream = storageProvider.getOfflineInputStream(bucketName, fileName);
        long totalRecords = StorageUtil.getCsvRecordCount(inputStream);
        return GcsCsvSplitUtil.getStorageSplitList(totalRecords, fileName, recordsPerSplit());
    }

    /**
     * Retrieves table data for provided arguments
     *
     * @param schema      Schema of the table
     * @param constraints Constraints if any
     * @param tableInfo   Table info containing table and schema name
     * @param split       Current Split instance
     */
    @Override
    public void readRecords(Schema schema, Constraints constraints, TableName tableInfo,
                            Split split, BlockSpiller spiller, QueryStatusChecker queryStatusChecker) throws IOException
    {
        String databaseName = tableInfo.getSchemaName();
        String tableName = tableInfo.getTableName();
        if (!storeCheckingComplete) {
            this.checkMetastoreForAll(databaseName);
        }
        String bucketName;
        List<String> objectNames = null;
        bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("No schema '" + databaseName + "' found");
        }
        Map<StorageObject, List<String>> tableObjectMap = tableObjects.get(databaseName);
        if (tableObjectMap != null) {
            objectNames = tableObjectMap.get(tableName);
        }
        if (objectNames == null) {
            throw new TableNotFoundException("No table '" + tableName + "' found under schema '" + databaseName + "'");
        }
        try {
            readRecordsForRequest(schema, constraints, tableInfo, split, bucketName, spiller, queryStatusChecker);
        }
        catch (Exception exception) {
            throw new UncheckedStorageDatasourceException("Error occurred during reading CSV records. Table name : " + tableName
                    + ", schema name: " + tableInfo.getSchemaName() + ", original file name(s): " + objectNames
                    + ", bucket name: " + bucketName + ". Error message: "
                    + exception.getMessage(), exception);
        }
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     * @throws IOException Raises if any occurs
     */
    @Override
    protected List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException
    {
        try {
            requireNonNull(objectNames, "List of tables in bucket " + bucketName + " was null");
            if (objectNames.isEmpty()) {
                throw new UncheckedStorageDatasourceException("List of tables in bucket " + bucketName + " was empty");
            }
            ImmutableList.Builder<Field> fieldListBuilder = ImmutableList.builder();
            List<String> fieldNames = inferSchemaFields(bucketName, objectNames.get(0));
            for (String field : fieldNames) {
                fieldListBuilder.add(new Field(field.toLowerCase(),
                        FieldType.nullable(new ArrowType.Utf8()), null));
            }
            return fieldListBuilder.build();
        }
        catch (Exception exception) {
            LOGGER.error("Unable to retrieve field schema for file(s) {}, under the bucket {}", objectNames,
                    bucketName);
            throw new UncheckedStorageDatasourceException(exception.getMessage(), exception);
        }
    }

    @Override
    public int recordsPerSplit()
    {
        return 10_000;
    }

    // helpers

    /**
     * Retrieves the field of the CSV data file from GCS bucket
     *
     * @param bucketName Name of the bucket
     * @param fileName   Csv file name
     * @return A list of field names
     * @throws IOException If occurs any during read or any IO operations
     */
    public List<String> inferSchemaFields(String bucketName, String fileName) throws IOException
    {
        try (InputStream inputStream = storageProvider.getOnlineInputStream(bucketName, fileName)) {
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            CsvParserSettings settings = new CsvParserSettings();
            settings.setHeaderExtractionEnabled(true); // grabs headers from input
            settings.setMaxCharsPerColumn(10240);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(reader);
            return Arrays.asList(parser.getRecordMetadata().headers());
        }
        catch (Exception exception) {
            throw new LoadSchemaFailedException("Schema of database(" + bucketName + "/" + fileName + ") "
                    + getValidEntityName(bucketName) + " failed", exception);
        }
    }

    private void readRecordsForRequest(Schema schema, Constraints constraints, TableName tableInfo,
                                       Split split, String bucketName,
                                       BlockSpiller spiller, QueryStatusChecker queryStatusChecker) throws IOException
    {
        CsvFilter csvFilter = new CsvFilter();
        ConstraintEvaluator evaluator = csvFilter.evaluator(schema, constraints, tableInfo, split);
        evaluator.withSpillerAndStatusChecker(spiller, queryStatusChecker);
        evaluator.setSchema(schema);
        final StorageSplit storageSplit
                = new ObjectMapper()
                .readValue(split.getProperty(STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
                        StorageSplit.class);
        try (InputStream inputStream = storageProvider.getOfflineInputStream(bucketName, storageSplit.getFileName())) {
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            String storageSplitJson = split.getProperty(STORAGE_SPLIT_JSON);
            requireNonNull(storageSplitJson, "Storage split JSON is required to to define the split");
            GroupSplit groupSplits = storageSplit.getGroupSplits().get(0);
            CsvParserSettings settings = new CsvParserSettings();
            settings.setHeaderExtractionEnabled(true); // grabs headers from input
            settings.setMaxCharsPerColumn(10240);
            settings.setProcessor(evaluator.processor());
            settings.selectFields(csvFilter.fields().toArray(new String[0]));
            settings.setColumnReorderingEnabled(false);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(reader);
            RecordMetaData metaData = parser.getRecordMetadata();
            evaluator.recordMetadata(metaData);
            if (groupSplits.getRowOffset() != 0) {
                evaluator.stop();
                skipRecords(evaluator, parser, groupSplits.getRowOffset() - 1);
            }
            long maxCount = groupSplits.getRowCount();
            int readCount = 0;
            while (parser.parseNext() != null) {
                readCount++;
                if (readCount == maxCount) {
                    break;
                }
            }
        }
        catch (Exception exception) {
            throw new ReadRecordsException("Unable to read records from file " + storageSplit.getFileName()
                    + " from bucket " + bucketName + ". Error message=" + exception.getMessage(), exception);
        }
    }

    private void skipRecords(ConstraintEvaluator evaluator, CsvParser parser, long count)
    {
        int skippedCount = 0;
        while (parser.parseNext() != null) {
            skippedCount++;
            if (skippedCount == count) {
                evaluator.resume();
                break;
            }
        }
    }

    private void checkFilesSize(String bucket, String objectName)
    {
        if (storageProvider.getFileSize(bucket, objectName) > MAX_CSV_FILES_SIZE) {
            throw new UncheckedStorageDatasourceException("Length of the CSV file '" + objectName + "' exceeds the maximum allowed size "
                    + humanReadableByteCountBin());
        }
    }

    private String humanReadableByteCountBin()
    {
        long absB = Math.abs(MAX_CSV_FILES_SIZE);
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(MAX_CSV_FILES_SIZE);
        return String.format("%.1f %cB", value / 1024.0, ci.current());
    }
}
