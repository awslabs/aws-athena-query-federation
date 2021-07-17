/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.deltalake;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.github.mjakubowski84.parquet4s.*;
import com.google.common.primitives.Longs;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None;
import software.amazon.ion.Decimal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.connectors.athena.deltalake.DeltalakeMetadataHandler.*;
import static java.lang.String.format;

public class DeltalakeRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltalakeRecordHandler.class);

    private static final String SOURCE_TYPE = "deltalake";

    public DeltalakeRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
    }

    @VisibleForTesting
    protected DeltalakeRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
    }

    protected Optional<Value> getValue(Object context, String fieldName) {
        RowParquetRecord record = (RowParquetRecord)context;
        Value value = record.get(fieldName);
        if (value instanceof NullValue$) return Optional.empty();
        else return Optional.of(value);
    }

    protected Extractor getNumberExtractor(int bitWidth, String fieldName, Optional<Object> literalValue) {
        if (bitWidth == 8 * NullableTinyIntHolder.WIDTH) return new TinyIntExtractor() {
            @Override
            public void extract(Object context, NullableTinyIntHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (byte)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((PrimitiveValue<Byte>)v).value()).orElse(Byte.valueOf("0"));
                }
            }
        };
        else if (bitWidth == 8 * NullableSmallIntHolder.WIDTH) return new SmallIntExtractor() {
            @Override
            public void extract(Object context, NullableSmallIntHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (short)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((PrimitiveValue<Short>)v).value()).orElse(Short.valueOf("0"));
                }
            }
        };
        else if (bitWidth == 8 * NullableIntHolder.WIDTH) return new IntExtractor() {
            @Override
            public void extract(Object context, NullableIntHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (int)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((PrimitiveValue<Integer>)v).value()).orElse(0);
                }
            }
        };
        else if (bitWidth == 8 * NullableBigIntHolder.WIDTH) return new BigIntExtractor() {
            @Override
            public void extract(Object context, NullableBigIntHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                dst.value = parquetValue.map(v -> ((PrimitiveValue<Long>)v).value()).orElse(0L);
                if (literalValue.isPresent()) {
                    dst.value = (long)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((PrimitiveValue<Long>)v).value()).orElse(0L);
                }
            }
        };
        else throw new IllegalArgumentException("Unsupported bitWidth: " + bitWidth);
    }

    protected Extractor getFloatExtractor(FloatingPointPrecision precision, String fieldName, Optional<Object> literalValue) {
        if (precision == FloatingPointPrecision.SINGLE) return new Float4Extractor() {
            @Override
            public void extract(Object context, NullableFloat4Holder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (float)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((FloatValue)v).value()).orElse(0f);
                }
            }

        };
        else if (precision == FloatingPointPrecision.DOUBLE) return new Float8Extractor() {
            @Override
            public void extract(Object context, NullableFloat8Holder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (double)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((DoubleValue)v).value()).orElse(0d);
                }
            }
        };
        else throw new IllegalArgumentException("Unsupported float precision: " + precision);
    }

    public Extractor getExtractor(Field field) {
        return getExtractor(field, Optional.empty());
    }

    public Extractor getExtractor(Field field, Optional<Object> literalValue) {
        ArrowType fieldType = field.getType();
        String fieldName = field.getName();
        if (fieldType.getTypeID() == ArrowTypeID.Int) return getNumberExtractor(((ArrowType.Int)fieldType).getBitWidth(), fieldName, literalValue);
        else if (fieldType.getTypeID() == ArrowTypeID.Bool) return new BitExtractor(){
            @Override
            public void extract(Object context, NullableBitHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (int)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((BooleanValue)v).value() ? 1 : 0).orElse(0);
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowTypeID.Utf8) return new VarCharExtractor(){
            @Override
            public void extract(Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (String)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> ((BinaryValue)v).value().toStringUsingUTF8()).orElse("");
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowTypeID.Date) return new DateMilliExtractor() {
            @Override
            public void extract(Object context, NullableDateMilliHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (Long)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v -> Longs.fromByteArray(((BinaryValue)v).value().getBytes())).orElse(0L);
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowTypeID.FloatingPoint) return getFloatExtractor(((ArrowType.FloatingPoint)fieldType).getPrecision(), fieldName, literalValue);
        else if (fieldType.getTypeID() == ArrowTypeID.Decimal) return new DecimalExtractor() {
            @Override
            public void extract(Object context, com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder dst) throws Exception {
                Optional<Value> parquetValue = getValue(context, fieldName);
                dst.isSet = parquetValue.isPresent() ? 1 : 0;
                if (literalValue.isPresent()) {
                    dst.value = (Decimal)literalValue.get();
                } else {
                    dst.value = parquetValue.map(v ->
                            Decimals.decimalFromBinary(((BinaryValue) v).value(), Decimals.Scale(), Decimals.MathContext()).bigDecimal()).orElse(BigDecimal.valueOf(0));
                }
            }
        };
        else if (fieldType.getTypeID() == ArrowType.ArrowTypeID.Null) return new Extractor() {};
        else return new Extractor() {};
    }




    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        Split split = recordsRequest.getSplit();

        String relativeFilePath = split.getProperty(FILE_SPLIT_PROPERTY_KEY);

        List<String> partitionNames = deserializePartitionNames(split.getProperty(PARTITION_NAMES_SPLIT_PROPERTY_KEY));

        String tableName = recordsRequest.getTableName().getTableName();
        String schemaName = recordsRequest.getTableName().getSchemaName();

        String tablePath = String.format("s3a://%s/%s/%s/", DATA_BUCKET, schemaName, tableName);
        String filePath = String.format("%s%s", tablePath, relativeFilePath);

        List<Field> fields = recordsRequest.getSchema().getFields();

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        for(Field field : fields) {
            String fieldName = field.getName();
            if (partitionNames.contains(fieldName)) {
                String partitionValue = split.getProperty(getPartitionValuePropertyKey(fieldName));
                builder.withExtractor(fieldName, getExtractor(field, Optional.of(partitionValue)));
            }
            else builder.withExtractor(fieldName, getExtractor(field));
        }

        GeneratedRowWriter rowWriter = builder.build();
        ParquetReadSupport parquetReadSupport = new ParquetReadSupport();
        org.apache.parquet.hadoop.ParquetReader<RowParquetRecord> parquetReader = org.apache.parquet.hadoop.ParquetReader.builder(parquetReadSupport, new Path(filePath)).build();

        long countRecord = 0L;
        RowParquetRecord record;
        while((record = parquetReader.read()) != null) {
            RowParquetRecord finalRecord = record;
            spiller.writeRows((block, rowNum) -> rowWriter.writeRow(block, rowNum, finalRecord) ? 1 : 0);
            countRecord += 1;
        }
        logger.info("Split finished with %l records", countRecord);
    }
}
