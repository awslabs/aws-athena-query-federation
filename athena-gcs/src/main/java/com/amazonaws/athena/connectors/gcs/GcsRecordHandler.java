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

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.gcs.storage.StorageConstants;
import com.amazonaws.athena.connectors.gcs.storage.StorageDatasource;
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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.isFieldTypeNull;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;
import static com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory.createDatasource;

public class GcsRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandler.class);

    private static final String STORAGE_FILE = "s3://GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NXL5GOSRX6BA2F7RMA6YJGO3Q:haK0skzuPrUljknEsfcRJCYRXklVAh+LuaIiirh1@athena-integ-test-1/bing_covid-19_data.parquet?endpoint_override=https%3A%2F%2Fstorage.googleapis.com";

    private static final String SOURCE_TYPE = "gcs";

    private final StorageDatasource datasource;

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
        GcsUtil.installGoogleCredentialsJsonFile(gcsCredentialsJsonString);
        this.datasource = createDatasource(gcsCredentialsJsonString, System.getenv());
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
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
        Split split = recordsRequest.getSplit();
        final StorageSplit storageSplit
                = new ObjectMapper()
                .readValue(split.getProperty(StorageConstants.STORAGE_SPLIT_JSON).getBytes(StandardCharsets.UTF_8),
                        StorageSplit.class);
        System.out.println("Storage split to read data:\n" + storageSplit);
        String uri = createUri(storageSplit.getFileName());
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
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();

                        // Taking a fixed width (1 bit) vector of boolean values.
                        // We will pass it to gandiva's Projector that will evaluate the
                        // expressions against to recordBatch
                        // This is a reference about the result of filter application.
                        // By the row index will access it to know the result.
                        BitVector filterResult = new BitVector("", allocator)
                ) {
                    // Allocating memory for the vector,
                    // it is equal to the size of the records in the batch.
                    filterResult.allocateNew(root.getRowCount());

                    // Taking a helper to handle the conversion of VectorSchemaRoot to a ArrowRecordBatch.
                    final VectorUnloader vectorUnloader = new VectorUnloader(root);

                    // We need a holder for the result of applying the Project on the data.
                    List<ValueVector> filterOutput = new ArrayList<>();
                    filterOutput.add(filterResult);

                    // Getting converted ArrowRecordBatch from the helper.
                    try (ArrowRecordBatch batch = vectorUnloader.getRecordBatch()) {
                        // We will loop on batch records and consider each records to write in spiller.
                        for (int rowIndex = 0; rowIndex < root.getRowCount(); rowIndex++) {
                            // we are passing record to spiller to be written.
                            execute(spiller, root.getFieldVectors(), rowIndex);
                        }
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
     * @throws Exception - exception
     */
    private void execute(
            BlockSpiller spiller,
            List<FieldVector> gcsFieldVectors, int rowIndex) throws Exception
    {
        spiller.writeRows((Block block, int rowNum) -> {
            boolean isMatched;
            for (FieldVector vector : gcsFieldVectors) {
                Field field = vector.getField();
                // Default field type is VARCHAR if the type is NULL
                if (isFieldTypeNull(field)) {
                    field = Field.nullable(field.getName(), Types.MinorType.VARCHAR.getType());
                }
                Object value = vector.getObject(rowIndex);
                if (field.getName().equalsIgnoreCase("arrcol")) {
                    System.out.println("Field " + field.getName() + " is of type " + field.getType().getTypeID().toString());
                    value = coerceListField(field, value);
                }
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

    protected Object coerceListField(Field field, Object fieldValue)
            throws RuntimeException
    {
        if (fieldValue == null) {
            return null;
        }

        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        System.out.printf("Field %s is of type %s%n", field.getName(), fieldType);
        switch (fieldType) {
            case LIST:
                Field childField = field.getChildren().get(0);
                if (fieldValue instanceof List) {
                    // Both fieldType and fieldValue are lists => Return as a new list of values, applying coercion
                    // where necessary in order to match the type of the field being mapped into.
                    List<Object> coercedValues = new ArrayList<>();
                    ((List) fieldValue).forEach(value ->
                            coercedValues.add(coerceField(childField, value)));
                    return coercedValues;
                }
                else if (!(fieldValue instanceof Map)) {
                    // This is an abnormal case where the fieldType was defined as a list in the schema,
                    // however, the fieldValue returns as a single value => Return as a list of a single value
                    // applying coercion where necessary in order to match the type of the field being mapped into.
                    return Collections.singletonList(coerceField(childField, fieldValue));
                }
                break;
            default:
                break;
        }

        throw new RuntimeException("Invalid field value encountered in Document for field: " + field.toString() +
                ",value: " + fieldValue.toString());
    }

    private Object coerceField(Field field, Object fieldValue)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        if (fieldType != Types.MinorType.LIST && fieldValue instanceof List) {
            // This is an abnormal case where the field was not defined as a list in the schema,
            // however, fieldValue returns a list => return first item in list only applying coercion
            // where necessary in order to match the type of the field being mapped into.
            return coerceField(field, ((List) fieldValue).get(0));
        }

        switch (fieldType) {
            case LIST:
                return coerceListField(field, fieldValue);
            case BIGINT:
                if (!field.getMetadata().isEmpty() && field.getMetadata().containsKey("scaling_factor")) {
                    // scaled_float w/scaling_factor - a float represented as a long.
                    double scalingFactor = new Double(field.getMetadata().get("scaling_factor"));
                    if (fieldValue instanceof String) {
                        return Math.round(new Double((String) fieldValue) * scalingFactor);
                    }
                    else if (fieldValue instanceof Number) {
                        return Math.round(((Number) fieldValue).doubleValue() * scalingFactor);
                    }
                    break;
                }
                else if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).longValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).longValue();
                }
                break;
            case INT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).intValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).intValue();
                }
                break;
            case SMALLINT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).shortValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).shortValue();
                }
                break;
            case TINYINT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).byteValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).byteValue();
                }
                break;
            case FLOAT8:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).doubleValue();
                }
                break;
            case FLOAT4:
                if (fieldValue instanceof String) {
                    return new Float((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).floatValue();
                }
                break;
            case DATEMILLI:
                if (fieldValue instanceof String) {
                    try {
                        return toEpochMillis((String) fieldValue);
                    }
                    catch (DateTimeParseException error) {
                        LOGGER.warn("Error parsing localDateTime: {}.", error.getMessage());
                        return null;
                    }
                }
                if (fieldValue instanceof Number) {
                    // Date should be a long numeric value representing epoch milliseconds (e.g. 1589525370001).
                    return ((Number) fieldValue).longValue();
                }
                break;
            case BIT:
                if (fieldValue instanceof String) {
                    return new Boolean((String) fieldValue);
                }
                break;
            default:
                break;
        }

        return fieldValue;
    }

    private long toEpochMillis(String dateTimeValue)
            throws DateTimeParseException
    {
        long epochSeconds;
        double nanoSeconds;

        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateTimeValue,
                    DateTimeFormatter.ISO_ZONED_DATE_TIME.withResolverStyle(ResolverStyle.SMART));
            epochSeconds = zonedDateTime.toEpochSecond();
            nanoSeconds = zonedDateTime.getNano();
        }
        catch (DateTimeParseException error) {
            LocalDateTime localDateTime = LocalDateTime.parse(dateTimeValue,
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
                            .withResolverStyle(ResolverStyle.SMART));
            epochSeconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
            nanoSeconds = localDateTime.getNano();
        }

        return epochSeconds * 1000 + Math.round(nanoSeconds / 1000000);
    }
}
