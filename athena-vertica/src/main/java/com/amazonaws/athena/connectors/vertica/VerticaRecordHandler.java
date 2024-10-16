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
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_QUERY_ID;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;


public class VerticaRecordHandler
        extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(VerticaRecordHandler.class);
    private static final String SOURCE_TYPE = "vertica";

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
                Extractor extractor = makeExtractor(next, mapOfNamesAndTypes, mapOfCols);
                builder.withExtractor(next.getName(), extractor);
            }
            GeneratedRowWriter rowWriter = builder.build();

            /*
            Using Arrow Dataset to read the S3 Parquet file generated in the split
            */
            try (ArrowReader reader = constructArrowReader(constructS3Uri(exportBucket, s3ObjectKey)))
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
            } catch (Exception e) {
                throw new RuntimeException("Error in connecting to S3 and selecting the object content for object : " + s3ObjectKey, e);
            }
        }

    }


    /**
     * Creates an Extractor for the given field.
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
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = ((boolean) value) ? 1 : 0;
                        dst.isSet = 1;
                        }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Byte.parseByte(value.toString());
                        dst.isSet = 1;
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else{
                        dst.value = Short.parseShort(value.toString());
                        dst.isSet = 1;
                    }
                };
            case INT:
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Long.parseLong(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Float.parseFloat(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Double.parseDouble(value.toString());
                        dst.isSet = 1;
                    }
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = new BigDecimal(value.toString());
                        dst.isSet = 1;
                    }

                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else{
                        dst.isSet = 1;
                        dst.value = (int) LocalDate.parse(value.toString()).toEpochDay();
                    }

                };

            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName).toString();
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = LocalDateTime.parse(value.toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                        dst.isSet = 1;
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
                    Object value = ((RowContext) context).getNameValue().get(fieldName).toString();
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = value.toString().getBytes();
                        dst.isSet = 1;
                    }
                };
            default:
                throw new RuntimeException("Unhandled type " + fieldType);
        }
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
