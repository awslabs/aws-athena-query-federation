/*-
 * #%L
 * athena-storage-api
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
package com.amazonaws.athena.connectors.gcs.storage;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.gcs.common.FieldValue;
import com.amazonaws.athena.connectors.gcs.common.PartitionUtil;
import com.amazonaws.athena.connectors.gcs.common.StorageLocation;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.common.StorageSplit;
import com.amazonaws.athena.connectors.gcs.filter.EqualsExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.athena.connectors.gcs.glue.GlueUtil;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.createUri;
import static com.google.cloud.storage.Storage.BlobListOption.prefix;
import static java.util.Objects.requireNonNull;

public class StorageMetadata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageMetadata.class);
    protected static Storage storage;

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param gcsCredentialJsonString An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @param properties environment properties as key value pair
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    public StorageMetadata(String gcsCredentialJsonString,
                           Map<String, String> properties) throws IOException
    {
        requireNonNull(gcsCredentialJsonString, "GCS credential JSON is null");
        requireNonNull(properties, "Environment variables were null");
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(gcsCredentialJsonString.getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    /**
     * Returns a list of field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @param format   classification param form table
     * @return An instance of {@link List<Field>} with column metadata
     */
    public synchronized List<Field> getFields(String databaseName, String tableName, String format)
    {
        LOGGER.info("Getting table fields for object {}.{}", databaseName, tableName);
        List<String> files = getStorageFiles(databaseName, tableName, format);
        if (!files.isEmpty()) {
            return getFileSchema(databaseName, files.get(0), FileFormat.valueOf(format.toUpperCase())).getFields();
        }

        throw new IllegalArgumentException("No object found for the table name '" + tableName + "' under bucket " + databaseName);
    }

    /**
     * Retrieves a list of StorageSplit that essentially contain the list of all files for a given table type in a storage location
     *
     * @param tableType Type of the table (e.g., PARQUET or CSV)
     * @param partition List of {@link StorageLocation} instances
     * @return A list of {@link StorageSplit} instances
     */
    public List<StorageSplit> getStorageSplits(String tableType, StorageLocation partition)
    {
        String extension = "." + tableType.toLowerCase();
        List<StorageSplit> splits = new ArrayList<>();
        String bucketName = partition.getBucketName();
        Page<Blob> blobs = storage.list(bucketName, prefix(partition.getLocation()));
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().toLowerCase().endsWith(extension)) {
                splits.add(StorageSplit.builder().fileName(bucketName + "/" + blob.getName()).build());
            }
        }
        return splits;
    }

    /**
     *  Used to test with test classes integrated directly with GCS bucket
     *  <p>
     *      Those test classes are not part of the artifact and not present in src/test. However, one may use it to
     *      test few methods that requires real-life debugging
     *  </p>
     * @return An instance of {@link Storage} from Google Storage SDK
     */
    public Storage getStorage()
    {
        return storage;
    }

    /**
     * Retrieves a list of partition folders from the GCS bucket based on partition.pattern Table parameter and partition keys set forth in Glue table. If the summary from the
     * constraints is empty (no where clauses or unsupported clauses), it will essentially return all the partition folders from the GCS bucket. If there is any constraints to
     * apply, it will apply constraints to filter selected partition folder, to narrow down the data load
     *
     * TODO: Date expression evaluation needs to be taken care
     *
     * @param request An instance of {@link MetadataRequest}, may be {@link com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest} or any subclass
     * @param schema An instance of {@link Schema} that describes underlying Table's schema
     * @param tableInfo Name of the table
     * @param constraints An instance of {@link Constraints}, captured from where clauses
     * @param awsGlue An instance of {@link AWSGlue}
     * @return A list of {@link List<StoragePartition>} instances
     * @throws ParseException Throws if any occurs during parsing regular expression
     */
    public List<List<StoragePartition>> getPartitionFolders(MetadataRequest request, Schema schema, TableName tableInfo, Constraints constraints, AWSGlue awsGlue)
            throws ParseException
    {
        LOGGER.info("Getting partition folder(s) for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        List<List<StoragePartition>> partitionFolders = new ArrayList<>();
        // get Glue table object
        Table table = GlueUtil.getGlueTable(request, tableInfo, awsGlue);
        if (table != null) {
            // get partition folder regEx pattern
            Optional<String> optionalFolderRegEx = PartitionUtil.getRegExExpression(table);
            if (optionalFolderRegEx.isPresent()) {
                String locationUri = table.getStorageDescriptor().getLocation();
                LOGGER.info("Location URI for table {}.{} is {}", tableInfo.getSchemaName(), tableInfo.getTableName(), locationUri);
                StorageLocation storageLocation = StorageLocation.fromUri(locationUri);
                LOGGER.info("Listing object in location {} under the bucket {}", storageLocation.getLocation(), storageLocation.getBucketName());
                Page<Blob> blobPage = storage.list(storageLocation.getBucketName(), prefix(storageLocation.getLocation()));
                String folderRegEx = optionalFolderRegEx.get();
                Pattern folderRegExPattern = Pattern.compile(folderRegEx);
                List<EqualsExpression> expressions = new FilterExpressionBuilder(schema).getExpressions(constraints);
                LOGGER.info("Expressions for the request of {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), expressions);
                for (Blob blob : blobPage.iterateAll()) {
                    String blobName = blob.getName();
                    String folderPath = blobName.startsWith(storageLocation.getLocation())
                            ? blobName.replace(storageLocation.getLocation(), "")
                            : blobName;
                    LOGGER.info("Examining folder {} against regex {}", folderPath, folderRegEx);
                    if (folderRegExPattern.matcher(folderPath).matches()) {
                        LOGGER.info("Examining folder {} against regex {} matches", folderPath, folderRegEx);
                        if (!canIncludePath(folderPath, expressions)) {
                            LOGGER.info("Folder " + folderPath + " has NOT been selected against the expression");
                            continue;
                        }
                        LOGGER.info("Folder " + folderPath + " has been selected against the expression");
                        Map<String, String> tableParameters = table.getParameters();
                        List<StoragePartition> partitions = PartitionUtil.getStoragePartitions(tableParameters.get(PARTITION_PATTERN_PATTERN), folderPath,
                                folderRegEx, table.getPartitionKeys(), tableParameters);
                        if (!partitions.isEmpty()) {
                            partitionFolders.add(partitions);
                        }
                        else {
                            LOGGER.info("No partitions found for the folder {}", blob.getName());
                        }
                    }
                }
            }
        }
        else {
            LOGGER.info("Table {}.{} not found", tableInfo.getSchemaName(), tableInfo.getTableName());
        }
        return partitionFolders;
    }

    /**
     * Retrieves a list of files that match the extension as per the metadata config
     *
     * @param bucket Name of the bucket
     * @param prefix Prefix (aka, folder in Storage service) of the bucket from where this method with retrieve files
     * @return A list file names under the prefix
     */
    protected List<String> getStorageFiles(String bucket, String prefix, String format)
    {
        LOGGER.info("Listing nested files for prefix {} under the bucket {}", prefix, bucket);
        List<String> fileNames = new ArrayList<>();
        Page<Blob> blobPage = storage.list(bucket, prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            if (blob.getName().toLowerCase(Locale.ROOT).endsWith(format.toLowerCase(Locale.ROOT))) {
                fileNames.add(blob.getName());
            }
        }
        LOGGER.info("Files is prefix {} under the bucket {} are {}", prefix, bucket, fileNames);
        return fileNames;
    }

    // helpers
    public Schema getFileSchema(String bucketName, String fileName, FileFormat format)
    {
        requireNonNull(bucketName, "bucketName was null");
        requireNonNull(fileName, "fileName was null");
        LOGGER.info("Retrieving field schema from file {}, under the bucket {}", fileName, bucketName);
        String uri = createUri(bucketName, fileName);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory factory = new FileSystemDatasetFactory(allocator,
                NativeMemoryPool.getDefault(), format, uri);
        // inspect schema
        return factory.inspect();
    }

    private boolean canIncludePath(String folderPath, List<EqualsExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return true;
        }
        String[] folderPaths = folderPath.split("/");
        for (String path : folderPaths) {
            Optional<FieldValue> optionalFieldValue = FieldValue.from(path);
            if (optionalFieldValue.isEmpty()) {
                continue;
            }
            FieldValue fieldValue = optionalFieldValue.get();
            Optional<EqualsExpression> optionalExpression = expressions.stream()
                    .filter(expr -> expr.columnName().equalsIgnoreCase(fieldValue.getField()))
                    .findFirst();
            if (optionalExpression.isPresent()) {
                LOGGER.info("Evaluating field value {} against the expression {}", fieldValue, expressions);
                EqualsExpression expression = optionalExpression.get();
                if (!expression.apply(fieldValue.getValue())) {
                    return false;
                }
            }
            else {
                LOGGER.info("No expression found for field {}", fieldValue.getField());
            }
        }
        return true;
    }
}
