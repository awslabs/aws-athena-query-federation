/*-
 * #%L
 * athena-vertica
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

package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.dataset.file.FileFormat;
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
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_QUERY_ID;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;


public class VerticaRecordHandler
        extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(VerticaRecordHandler.class);
    private static final String SOURCE_TYPE = "vertica";

    // Base S3 client on the default credential chain. Reads are re-sourced onto the connection's
    // LF-vended (FAS-forwarded) session via getS3Client when the request carries FAS credentials;
    // otherwise getS3Client falls back to this client.
    private final S3Client amazonS3;

    // Parses Vertica read-back timestamps 'yyyy-MM-dd[ |T]HH:mm:ss[.fffffffff][offset]' (see
    // VerticaExportQueryBuilder): space or 'T' separator, optional fractional seconds and zone offset
    // ('+00'/'+00:00'/'Z'); no offset defaults to UTC.
    private static final DateTimeFormatter VERTICA_TIMESTAMP_READ_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart().appendLiteral(' ').optionalEnd()
            .optionalStart().appendLiteral('T').optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:mm", "Z")
            .optionalEnd()
            .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
            .toFormatter();

    public VerticaRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(), configOptions);
    }

    @VisibleForTesting
    protected VerticaRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient amazonAthena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);
        this.amazonS3 = amazonS3;
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException       Throws an IOException
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        logger.info("readWithConstraint: schema[{}] tableName[{}]", recordsRequest.getSchema(), recordsRequest.getTableName());

        Schema schemaName = recordsRequest.getSchema();
        Split split = recordsRequest.getSplit();
        String id = split.getProperty(VERTICA_SPLIT_QUERY_ID);
        String exportBucket = split.getProperty(VERTICA_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(VERTICA_SPLIT_OBJECT_KEY);

        if(!s3ObjectKey.isEmpty()) {
            //get column name and type from the Schema
            HashMap<String, Types.MinorType> mapOfNamesAndTypes = new HashMap<>();
            HashMap<String, Object> mapOfCols = new HashMap<>();

            for (Field field : schemaName.getFields()) {
                Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(field.getType());
                mapOfNamesAndTypes.put(field.getName(), minorTypeForArrowType);
                mapOfCols.put(field.getName(), null);
            }


            // creating a RowContext class to hold the column name and value.
            final RowContext rowContext = new RowContext(id);

            //Generating the RowWriter and Extractor
            GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
            for (Field next : recordsRequest.getSchema().getFields()) {
                // GeneratedRowWriter has no timestamp extractor, but honors a registered FieldWriterFactory
                // before its type switch; timestamps route here, all other types keep the makeExtractor path.
                if (Types.getMinorTypeForArrowType(next.getType()) == Types.MinorType.TIMESTAMPMICROTZ) {
                    builder.withFieldWriterFactory(next.getName(), makeTimestampMicroTzFieldWriterFactory());
                }
                else {
                    Extractor extractor = makeExtractor(next, mapOfNamesAndTypes, mapOfCols);
                    builder.withExtractor(next.getName(), extractor);
                }
            }
            GeneratedRowWriter rowWriter = builder.build();

            /*
            Using Arrow Dataset to read the S3 Parquet file generated in the split. When the request carries
            LakeFormation-vended (FAS-forwarded) credentials, download the export object with those creds via
            the SDK to a local temp file and read that; otherwise read it in place over Arrow's native S3
            filesystem on the default credential chain (prior behavior). The temp file is always cleaned up.
            */
            AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(recordsRequest);
            Path downloadedExport = null;
            try {
                String readerUri;
                if (overrideConfig != null && overrideConfig.credentialsProvider().isPresent()) {
                    // LF-vended creds present: fetch the export object as the federated identity, then read locally.
                    downloadedExport = Files.createTempFile("vertica-export-", ".parquet");
                    downloadExportObject(overrideConfig, exportBucket, s3ObjectKey, downloadedExport);
                    readerUri = downloadedExport.toUri().toString();
                }
                else {
                    logger.debug("No LakeFormation-vended (FAS) credentials present for this request; "
                            + "reading Vertica export object directly via the default credential chain");
                    readerUri = constructS3Uri(exportBucket, s3ObjectKey);
                }

                try (ArrowReader reader = constructArrowReader(readerUri))
                {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        for (int row = 0; row < root.getRowCount(); row++) {
                            HashMap<String, Object> map = new HashMap<>();
                            for (Field field : root.getSchema().getFields()) {
                                map.put(field.getName(), root.getVector(field).getObject(row));
                            }
                            rowContext.setNameValue(map);

                            //Passing the RowContext to BlockWriter;
                            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, rowContext) ? 1 : 0);
                        }
                    }
                    reader.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("Error in connecting to S3 and selecting the object content for object : " + s3ObjectKey, e);
            } finally {
                // Always remove the downloaded temp file, even on read failure. Only set when we downloaded.
                if (downloadedExport != null) {
                    try {
                        Files.deleteIfExists(downloadedExport);
                    }
                    catch (IOException ioe) {
                        logger.warn("Failed to delete temporary Vertica export file {}", downloadedExport, ioe);
                    }
                }
            }
        }

    }


    /**
     * Creates an Extractor for the given field. Read-back cells arrive as Arrow Text, so each
     * non-VARCHAR extractor parses the string form into its native type (a direct cast would throw).
     */
    private Extractor makeExtractor(Field field, HashMap<String, Types.MinorType> mapOfNamesAndTypes, HashMap<String, Object> mapOfcols)
    {
        String fieldName = field.getName();
        Types.MinorType fieldType = mapOfNamesAndTypes.get(fieldName);
        switch (fieldType)
        {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = parseVerticaBoolean(value, fieldName) ? 1 : 0;
                        dst.isSet = 1;
                    }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            dst.value = Byte.parseByte(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "TINYINT", e);
                        }
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            dst.value = Short.parseShort(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "SMALLINT", e);
                        }
                    }
                };
            case INT:
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            // INT and BIGINT share the long-valued BigInt holder, so Long.parseLong covers both.
                            dst.value = Long.parseLong(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "INT/BIGINT", e);
                        }
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            dst.value = Float.parseFloat(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "FLOAT4", e);
                        }
                    }
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            dst.value = Double.parseDouble(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "FLOAT8", e);
                        }
                    }
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            // DecimalFieldWriter applies the field's scale, so an unscaled BigDecimal is sufficient (no pre-scale).
                            dst.value = new BigDecimal(value);
                            dst.isSet = 1;
                        }
                        catch (NumberFormatException e) {
                            throw parseException(fieldName, value, "DECIMAL", e);
                        }
                    }
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null) {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            // DATE arrives as ISO 'yyyy-MM-dd' text; convert to epoch-day.
                            dst.value = (int) LocalDate.parse(value).toEpochDay();
                            dst.isSet = 1;
                        }
                        catch (DateTimeParseException e) {
                            throw parseException(fieldName, value, "DATEDAY", e);
                        }
                    }
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        try {
                            // Timestamp text parsed via VERTICA_TIMESTAMP_READ_FORMATTER; offset honored, else UTC.
                            dst.value = OffsetDateTime.parse(value, VERTICA_TIMESTAMP_READ_FORMATTER)
                                    .toInstant().toEpochMilli();
                            dst.isSet = 1;
                        }
                        catch (DateTimeParseException e) {
                            throw parseException(fieldName, value, "DATEMILLI/timestamp", e);
                        }
                    }
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else{
                        dst.value = value.toString();
                        dst.isSet = 1;
                    }
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    String value = getTrimmedCellValue(context, fieldName);
                    if (value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        // EXPORT renders binary content as text, so we take its UTF-8 bytes; arbitrary-binary
                        // round-trip is not guaranteed (a non-text payload would not survive the text export).
                        dst.value = value.getBytes(StandardCharsets.UTF_8);
                        dst.isSet = 1;
                    }
                };
            default:
                throw new RuntimeException("Unhandled type " + fieldType);
        }
    }

    /**
     * Builds a FieldWriterFactory for TIMESTAMPMICROTZ columns: parses the text cell with
     * VERTICA_TIMESTAMP_READ_FORMATTER and writes full-precision epoch-micros into the vector (UTC).
     */
    private FieldWriterFactory makeTimestampMicroTzFieldWriterFactory()
    {
        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
        {
            // extractor arg is null: readWithConstraint registered this factory INSTEAD of an extractor.
            String fieldName = vector.getField().getName();
            TimeStampMicroTZVector tsVector = (TimeStampMicroTZVector) vector;
            return (Object context, int rowNum) ->
            {
                String value = getTrimmedCellValue(context, fieldName);
                if (value == null) {
                    tsVector.setNull(rowNum);
                    // Mirror DateMilliFieldWriter: offer null to the constraint.
                    return constraint == null || constraint.apply(null);
                }
                try {
                    // Parse to an Instant, then write epoch-micros directly (full microsecond precision, UTC).
                    Instant instant = OffsetDateTime.parse(value, VERTICA_TIMESTAMP_READ_FORMATTER).toInstant();
                    long epochMicros = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
                    tsVector.setSafe(rowNum, epochMicros);
                    // Reconstruct the ZonedDateTime from epochMicros (SDK helper only unpacks millisecond precision).
                    return constraint == null
                            || constraint.apply(Instant.EPOCH.plus(epochMicros, ChronoUnit.MICROS).atZone(ZoneOffset.UTC));
                }
                catch (DateTimeParseException e) {
                    throw parseException(fieldName, value, "TIMESTAMPMICROTZ", e);
                }
            };
        };
    }

    /**
     * Returns the trimmed string form of the column's cell value, or null when the cell is null or
     * (after trimming) empty - so empty cells are marked not-set rather than failing to parse.
     */
    private static String getTrimmedCellValue(Object context, String fieldName)
    {
        Object value = ((RowContext) context).getNameValue().get(fieldName);
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    /**
     * Parses a BIT/boolean cell case-insensitively (true/false, t/f, 1/0); throws on any other input.
     */
    private static boolean parseVerticaBoolean(String value, String fieldName)
    {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "true":
            case "t":
            case "1":
                return true;
            case "false":
            case "f":
            case "0":
                return false;
            default:
                throw parseException(fieldName, value, "BIT/boolean", null);
        }
    }

    /**
     * Builds a RuntimeException naming the column, value, and target type of a failed read-back parse.
     */
    private static RuntimeException parseException(String fieldName, String value, String targetType, Throwable cause)
    {
        String message = String.format(
                "Failed to parse value [%s] for column [%s] as %s in the Vertica parquet read-back",
                value, fieldName, targetType);
        return (cause == null) ? new RuntimeException(message) : new RuntimeException(message, cause);
    }

    private static class RowContext
    {

        private final String queryId;
        private HashMap<String, Object> nameValue;

        public RowContext(String queryId){
            this.queryId = queryId;
        }

        public void setNameValue(HashMap<String, Object> map){
            this.nameValue = map;
        }
        public HashMap<String, Object> getNameValue() {
            return this.nameValue;
        }
    }

    /**
     * Streams the exported parquet object to a local file using the connection's LF-vended
     * (FAS-forwarded) session, so the read runs as the connector's federated identity rather than the
     * Lambda execution role. The object is copied straight to disk and never fully buffered in memory.
     * Callers gate this on a present override; getS3Client only falls back to the base client when the
     * override lacks credentials.
     */
    private void downloadExportObject(AwsRequestOverrideConfiguration overrideConfig, String bucket, String key, Path destination)
            throws IOException
    {
        S3Client s3Client = getS3Client(overrideConfig, amazonS3);
        try (ResponseInputStream<GetObjectResponse> objectStream = s3Client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build())) {
            Files.copy(objectStream, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @VisibleForTesting
    protected ArrowReader constructArrowReader(String uri)
    {
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                uri);
        Dataset dataset = datasetFactory.finish();
        ScanOptions options = new ScanOptions(/*batchSize*/ 32768);
        Scanner scanner = dataset.newScan(options);
        return scanner.scanBatches();
    }

    private static String constructS3Uri(String bucket, String key)
    {
        return "s3://" + bucket + "/" + key;
    }

}
