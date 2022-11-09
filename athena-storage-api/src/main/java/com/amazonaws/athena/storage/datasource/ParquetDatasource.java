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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.StorageConstants;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.amazonaws.athena.storage.common.StorageObjectField;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.amazonaws.athena.storage.datasource.parquet.column.GcsGroupRecordConverter;
import com.amazonaws.athena.storage.datasource.parquet.filter.ConstraintEvaluator;
import com.amazonaws.athena.storage.datasource.parquet.filter.ParquetFilter;
import com.amazonaws.athena.storage.gcs.GcsParquetSplitUtil;
import com.amazonaws.athena.storage.gcs.GroupSplit;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.google.common.base.Stopwatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.storage.StorageConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.storage.gcs.ParquetUtil.PARQUET_MAGIC_BYTES_STRING;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.filter.PagedRecordFilter.page;

@ThreadSafe
public class ParquetDatasource
        extends AbstractStorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetDatasource.class);

    private GcsGroupRecordConverter gcsGroupRecordConverter;

    /**
     * This constructor, as of now, is invoked to instantiate an instance of ParquetDatasource reflectively
     *
     * @param gcsCredentialJsonString Google Cloud Storage credential JSON to access GCS
     * @param properties              Map of property/value from lambda environment
     * @throws IOException If any occurs
     */
    @SuppressWarnings("unused")
    public ParquetDatasource(String gcsCredentialJsonString,
                             Map<String, String> properties) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this(new StorageDatasourceConfig()
                .credentialsJson(gcsCredentialJsonString)
                .properties(properties));
    }

    /**
     * Instantiates a ParquetDatasource based on properties found in the GcsDatasourceConfig instance, such as
     * file_extension
     *
     * @param config An instance of GcsDatasourceConfig
     * @throws IOException If any occurs
     */
    public ParquetDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        super(config);
    }

    /**
     * Indicates whether a ths datasource supports grouping of multiple files to form a single table
     *
     * @return This datasource supports reading multiple file to form a single table. So it always returns true
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
        requireNonNull(objectName, "Table " + objectName + " in bucket " + bucket + " was null");
        InputFile inputFile = storageProvider.getInputFile(bucket, objectName);
        try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
            ParquetMetadata metadata = reader.getFooter();
            MessageType messageType = metadata.getFileMetaData().getSchema();
            List<ColumnDescriptor> columnDescriptors = messageType.getColumns();
                    List<StorageObjectField> fieldList = new ArrayList<>();
            for (int i = 0; i < columnDescriptors.size(); i++) {
                ColumnDescriptor columnDescriptor = columnDescriptors.get(i);
                fieldList.add(StorageObjectField.builder()
                                .columnName(columnDescriptor.getPath()[0])
                                .columnIndex(i)
                        .build());
            }
            return StorageObjectSchema.builder()
                    .fields(fieldList)
                    .baseSchema(messageType)
                    .build();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FilterExpression> getExpressions(String bucket, String objectName, Schema schema, TableName tableName, Constraints constraints,
                                                 Map<String, String> partitionFieldValueMap) throws IOException
    {
        StorageObjectSchema objectSchema = getObjectSchema(bucket, objectName);
        return new ParquetFilter(schema, objectSchema, partitionFieldValueMap)
                .evaluator(tableName, partitionFieldValueMap, constraints)
                .getExpressions();
    }

    @Override
    public boolean isSupported(String bucket, String objectName) throws IOException
    {
        boolean isWithValidExtension = objectName.toLowerCase().endsWith(datasourceConfig.extension());
        if (!isWithValidExtension) {
            InputFile inputFile = storageProvider.getInputFile(bucket, objectName);
            try (SeekableInputStream inputStream = inputFile.newStream()) {
                inputStream.seek(0L);
                byte[] initBytes = new byte[4];
                int readSize = inputStream.read(initBytes);
                if (readSize == 4) {
                    return PARQUET_MAGIC_BYTES_STRING.equals(new String(initBytes));
                }
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getBaseName(String bucket, String objectName)
    {
        return storageProvider.getFirstObjectNameRecurse(bucket, objectName);
    }

    @Override
    public List<StorageSplit> getSplitsByStoragePartition(StoragePartition partition)
    {
        return List.of();
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
     * @throws IOException Raised if any raised during connecting to the cloud storage
     */
    @Override
    public List<StorageSplit> getStorageSplits(Schema schema, Constraints constraints, TableName tableInfo,
                                               String bucketName, String objectNames) throws IOException
    {
        String[] fileNames = objectNames.split(",");
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            try {
                InputFile inputFile = storageProvider.getInputFile(bucketName, fileName);
                try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
                    splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName,
                            reader, recordsPerSplit()));
                }
            }
            catch (Exception exception) {
                throw new UncheckedStorageDatasourceException(exception.getMessage(), exception);
            }
        }
        return splits;
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void readRecords(Schema schema, Constraints constraints, TableName tableInfo,
                            Split split, BlockSpiller spiller, QueryStatusChecker queryStatusChecker) throws IOException
    {
        String databaseName = tableInfo.getSchemaName();
        if (!storeCheckingComplete) {
            this.checkMetastoreForAll(databaseName);
        }
        String bucketName;
        String fileNames = split.getProperty(TABLE_PARAM_OBJECT_NAME);
        requireNonNull(fileNames, "No tables found under schema '" + databaseName + "'");
        String[] fileNameArray = fileNames.split(",");
        if (fileNameArray.length == 0) {
            throw new UncheckedStorageDatasourceException("No tables found under schema '" + databaseName + "'");
        }
        bucketName = split.getProperty(TABLE_PARAM_BUCKET_NAME);
        if (bucketName == null) {
            throw new UncheckedStorageDatasourceException("No schema '" + databaseName + "' found");
        }
        try {
            final StorageSplit storageSplit
                    = new ObjectMapper()
                    .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
                            StorageSplit.class);
            readRecords(schema, tableInfo, split, constraints, bucketName, storageSplit.getFileName(), spiller,
                    queryStatusChecker);
        }
        catch (IOException exception) {
            throw new UncheckedStorageDatasourceException(exception.getMessage(), exception);
        }
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     */
    @Override
    protected List<Field> getTableFields(String bucketName, List<String> objectNames)
    {
        try {
            requireNonNull(objectNames, "List of tables in bucket " + bucketName + " was null");
            if (objectNames.isEmpty()) {
                throw new UncheckedStorageDatasourceException("List of tables in bucket " + bucketName + " was empty");
            }
            InputFile inputFile = storageProvider.getInputFile(bucketName, objectNames.get(0));
            try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
                ParquetMetadata metadata = reader.getFooter();
                TypeFactory.FieldResolver fieldResolver = TypeFactory.filedResolver(metadata);
                return fieldResolver.resolveFields();
            }
        }
        catch (Exception exception) {
            LOGGER.error("Unable to retrieve field schema for file(s) {}, under the bucket {}", objectNames,
                    bucketName);
            throw new UncheckedStorageDatasourceException(exception.getMessage(), exception);
        }
    }

    /**
     * {{@inheritDoc}}
     *
     * @return
     */
    @Override
    public int recordsPerSplit()
    {
        return 200_000;
    }

    // helpers
    /**
     * Retrieves records for given constraints using parquet file reader instead of parquet reader
     *
     * @param split      An instance of Split that contains information of the file along with group, offset and size information
     * @param bucketName Name of the bucket
     * @see org.apache.parquet.hadoop.ParquetFileReader
     */
    private void readRecords(Schema schema, TableName tableInfo, Split split, Constraints constraints,
                             String bucketName, String objectName, BlockSpiller spiller,
                             QueryStatusChecker queryStatusChecker)
    {
        try {
            Stopwatch timer = Stopwatch.createStarted();
            final StorageSplit storageSplit
                    = new ObjectMapper()
                    .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
                            StorageSplit.class);
            InputFile inputFile = storageProvider.getInputFile(bucketName, storageSplit.getFileName());
            try (ParquetFileReader reader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
                MessageType messageType = reader.getFileMetaData().getSchema();
                Configuration configuration = new Configuration();
                configuration.set(ReadSupport.PARQUET_READ_SCHEMA, messageType.toString());
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(messageType);
                List<GroupSplit> groupSplits = storageSplit.getGroupSplits();
                for (GroupSplit groupSplit : groupSplits) {
                    PageReadStore pages = reader.readRowGroup(groupSplit.getGroupIndex());
                    FilterCompat.Filter filter = FilterCompat.get(page(groupSplit.getRowOffset(), groupSplit.getRowCount()));
                    if (pages != null && queryStatusChecker.isQueryRunning()) {
                        ParquetFilter parquetFilter = new ParquetFilter(schema, messageType, split);
                        ConstraintEvaluator evaluator = parquetFilter.evaluator(tableInfo, split, constraints);
                        gcsGroupRecordConverter = new GcsGroupRecordConverter(messageType, evaluator);
                        addRecords(schema, messageType, columnIO.getRecordReader(pages,
                                gcsGroupRecordConverter, filter), objectName, spiller, queryStatusChecker);
                    }
                }
            }
            finally {
                timer.stop();
                LOGGER.debug("Time took to read records: {}", timer.elapsed(TimeUnit.SECONDS));
            }
        }
        catch (Exception e) {
            throw new UncheckedStorageDatasourceException(e.getMessage(), e);
        }
    }

    /**
     * Add a record when not being filtered
     *
     * @param messageType       An instance of Schema with selected fields and associates types
     * @param groupRecordReader A reader to field/value from Group
     */
    private void addRecords(Schema schema, MessageType messageType, RecordReader<Group> groupRecordReader, String partFileName,
                            BlockSpiller spiller, QueryStatusChecker queryStatusChecker)
    {
        Group group;
        try {
            TypeFactory.ValueResolver valueResolver = TypeFactory.valueResolver(messageType);
            while ((group = groupRecordReader.read()) != null) {
                if (queryStatusChecker.isQueryRunning()
                        && (gcsGroupRecordConverter == null || !gcsGroupRecordConverter.shouldSkipCurrent())) {
                    Map<String, Object> record = valueResolver.getRecord(group);
                    record.put(BLOCK_PARTITION_COLUMN_NAME, partFileName);
                    spiller.writeRows((Block block, int rowNum) -> {
                        boolean isMatched = true;
                        for (final Field field : schema.getFields()) {
                            Object fieldValue = record.get(field.getName());
                            isMatched &= block.offerValue(field.getName(), rowNum, fieldValue);
                        }
                        return isMatched ? 1 : 0;
                    });
                }
            }
        }
        catch (Exception exception) {
            // ignored
            LOGGER.error("Error in iterating records. Error message {}", exception.getMessage());
            exception.printStackTrace();
        }
    }
}
