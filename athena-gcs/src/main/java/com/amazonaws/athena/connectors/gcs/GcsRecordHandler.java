/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.gcs.storage.StorageConstants;
import com.amazonaws.athena.connectors.gcs.storage.StorageMetadata;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;
import static com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory.createDatasource;

public class GcsRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandler.class);

    private static final String SOURCE_TYPE = "gcs";

    private final StorageMetadata datasource;

    public GcsRecordHandler() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient());
    }

    /**
     * Constructor that provides access to S3, secret manager and athena
     * <p>
     * @param amazonS3       An instance of AmazonS3
     * @param secretsManager An instance of AWSSecretsManager
     * @param amazonAthena   An instance of AmazonAthena
     */
    @VisibleForTesting
    protected GcsRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        String gcsCredentialsJsonString = getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split - it contains information
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker) throws Exception
    {
        TableName tableInfo = recordsRequest.getTableName();
        LOGGER.info("Reading records from the table {} under the schema {}", tableInfo.getTableName(), tableInfo.getSchemaName());
        Split split = recordsRequest.getSplit();
        final StorageSplit storageSplit
                = new ObjectMapper()
                .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
                        StorageSplit.class);
        String uri = createUri(storageSplit.getFileName());
        LOGGER.info("Retrieving records from the URL {} for the table {}.{}", uri, tableInfo.getSchemaName(), tableInfo.getTableName());
        ScanOptions options = new ScanOptions(32768);
        try (
                // Taking an allocator for using direct memory for Arrow Vectors/Arrays.
                // We will use this allocator for filtered result output.
                BufferAllocator allocator = new RootAllocator();

                // DatasetFactory provides a way to inspect a Dataset potential schema before materializing it.
                // Thus, we can peek the schema for data sources and decide on a unified schema.
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(), datasource.getFileFormat(), uri
                );

                // Creates a Dataset with auto-inferred schema
                Dataset dataset = datasetFactory.finish();

                // Create a new Scanner using the provided scan options.
                // This scanner also contains the arrow schema for the dataset.
                Scanner scanner = dataset.newScan(options);

                // To read Schema and ArrowRecordBatches we need a reader.
                // This reader reads the dataset as a stream of record batches.
                ArrowReader reader = scanner.scanBatches()
        ) {
            // We are loading records batch by batch until we reached at the end.
            while (reader.loadNextBatch()) {
                try (
                        // Returns the vector schema root.
                        // This will be loaded with new values on every call to loadNextBatch on the reader.
                        VectorSchemaRoot root = reader.getVectorSchemaRoot()
                ) {
                    // We will loop on batch records and consider each records to write in spiller.
                    for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
                        // we are passing record to spiller to be written.
                        execute(spiller, root.getFieldVectors(), rowIndex);
                    }
                }
            }
        }
    }

    /**
     * We are writing data to spiller. This function received the whole batch
     * along with row index. We will access into batch using the row index and
     * get the record to write into spiller.
     *
     * @param spiller - block spiller
     * @param gcsFieldVectors - the batch
     * @param rowIndex - row index
     */
    private void execute(
            BlockSpiller spiller,
            List<FieldVector> gcsFieldVectors, int rowIndex)
    {
        spiller.writeRows((Block block, int rowNum) -> {
            boolean isMatched;
            for (FieldVector vector : gcsFieldVectors) {
                Object value = vector.getObject(rowIndex);
                // Writing data in spiller for each field.
                isMatched = block.offerValue(vector.getField().getName(), rowNum, value);

                // If this field is not qualified we are not trying with next field,
                // just leaving the whole record.
                if (!isMatched) {
                    return 0;
                }
            }
            return 1;
        });
    }
}
