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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;


public class VerticaRecordHandler
        extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(VerticaRecordHandler.class);
    private static final String SOURCE_TYPE = "vertica";
    private static final String VERTICA_QUOTE_CHARACTER = "\"";
    private static final String QUERY = "select * from S3Object s";
    private AmazonS3 amazonS3;

    public VerticaRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(), configOptions);
    }

    @VisibleForTesting
    protected VerticaRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena, java.util.Map<String, String> configOptions)
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
        String id = split.getProperty("query_id");
        String exportBucket = split.getProperty("exportBucket");
        String s3ObjectKey = split.getProperty("s3ObjectKey");

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
         Using S3 Select to read the S3 Parquet file generated in the split
         */
            //Creating the read Request
            SelectObjectContentRequest request = generateBaseParquetRequest(exportBucket, s3ObjectKey);
            try (SelectObjectContentResult result = amazonS3.selectObjectContent(request)) {
                InputStream resultInputStream = result.getPayload().getRecordsInputStream();
                BufferedReader streamReader = new BufferedReader(new InputStreamReader(resultInputStream, StandardCharsets.UTF_8));
                String inputStr;
                while ((inputStr = streamReader.readLine()) != null) {
                    HashMap<String, Object> map = new HashMap<>();
                    //we are reading the parquet files, but serializing the output it as JSON as SDK provides a Parquet InputSerialization, but only a JSON or CSV OutputSerializatio
                    ObjectMapper objectMapper = new ObjectMapper();
                    map = objectMapper.readValue(inputStr, HashMap.class);
                    rowContext.setNameValue(map);

                    //Passing the RowContext to BlockWriter;
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, rowContext) ? 1 : 0);
                }
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


    /*
    Method to create the Parquet read request
     */
    private static SelectObjectContentRequest generateBaseParquetRequest(String bucket, String key)
    {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(VerticaRecordHandler.QUERY);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setParquet(new ParquetInput());
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setJson(new JSONOutput());
        request.setOutputSerialization(outputSerialization);

        return request;
    }

}
