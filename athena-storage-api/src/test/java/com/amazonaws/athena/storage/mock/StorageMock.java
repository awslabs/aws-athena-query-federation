/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.mock;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.storage.StorageConstants;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.datasource.ParquetDatasource;
import com.amazonaws.athena.storage.gcs.GroupSplit;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.PageImpl;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT4;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

public class StorageMock
{
    protected final static Map<String, String> parquetProps = new HashMap<>();
    public static final Map<String, String> properties = Map.of(
            "max_partitions_size", "100",
            "records_per_split", "1000000"
    );

    public final Map<String, ArrowType> parquetFieldMap = Map.of(
            "id", BIGINT.getType(),
            "name", VARCHAR.getType(),
            "dob", DATEDAY.getType(),
            "salary", FLOAT8.getType(),
            "create_at", VARCHAR.getType(),
            "tax_rate", FLOAT8.getType(),
            "onboarded", BIT.getType(),
            "sequence", INT.getType(),
            "factors", FLOAT4.getType()

    );

    public final Map<String, ArrowType> csvFieldMap = Map.of(
            "EMPLOYEEID", VARCHAR.getType(),
            "WEEKID", VARCHAR.getType(),
            "NAME", VARCHAR.getType(),
            "EMAIL", VARCHAR.getType(),
            "ADDRESS1", VARCHAR.getType(),
            "ADDRESS2", VARCHAR.getType(),
            "CITY", VARCHAR.getType(),
            "STATE", VARCHAR.getType(),
            "ZIPCODE", VARCHAR.getType()
    );

    @Mock
    private PageImpl<Bucket> blob;

    @Mock
    public FederatedIdentity federatedIdentity;

    public static void setUpBeforeClass()
    {
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "parquet");
        parquetProps.putAll(properties);
    }

    public Storage mockStorageWithBlobIterator(String bucketName)
    {
        Storage storage = mock(Storage.class);
        Bucket bucket = mock(Bucket.class);
        PowerMockito.when(storage.list()).thenReturn(blob);
        PowerMockito.when(blob.iterateAll()).thenReturn(List.of(bucket));
        PowerMockito.when(bucket.getName()).thenReturn(bucketName);
        return storage;
    }

    public ParquetDatasource mockParquetDatasource() throws IOException
    {
        ParquetDatasource parquetDatasource = Mockito.mock(ParquetDatasource.class);
        mockStorageSplit(parquetDatasource);
        return parquetDatasource;
    }

    public Storage mockReadableStorage(String bucketName)
    {
        Storage storage = mockStorageWithBlobIterator(bucketName);
        PowerMockito.when(storage.get(Mockito.any(BlobId.class))).thenReturn(mock(Blob.class));
        PowerMockito.when(storage.reader(Mockito.any(BlobId.class))).thenReturn(mock(ReadChannel.class));
        return storage;
    }

    // helpers
    public void mockStorageSplit(StorageDatasource datasource) throws IOException
    {
        List<StorageSplit> splits = List.of(StorageSplit.builder()
                .fileName("test.csv")
                .groupSplits(List.of(
                        GroupSplit.builder()
                                .groupIndex(0)
                                .rowOffset(0)
                                .rowCount(100)
                                .build()
                ))
                .build()
        );
        when(datasource.getStorageSplits(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(splits);

        when(datasource.getStorageSplits(Mockito.any(), Mockito.any()))
                .thenReturn(splits);
    }

    public synchronized GcsReadRecordsRequest buildReadRecordsRequest(Map<String, ValueSet> summary,
                                                                      String schema, String table, StorageSplit split,
                                                                      boolean parquetFields) throws JsonProcessingException
    {
        TableName inputTableName = new TableName(schema, table);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        addSchemaFields(schemaBuilder, parquetFields);
        Schema fieldSchema = schemaBuilder.build();
        Constraints constraints = new GcsConstraints(summary);
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add(StorageConstants.STORAGE_SPLIT_JSON, new ObjectMapper().writeValueAsString(split))
                .add(TABLE_PARAM_OBJECT_NAME_LIST, split.getFileName())
                .add(TABLE_PARAM_BUCKET_NAME, table);
        return new GcsReadRecordsRequest(this.federatedIdentity,
                "default", "testQueryId", inputTableName,
                fieldSchema, splitBuilder.build(), constraints, 1024, 1024);
    }

    public Map<String, ValueSet> createSummaryWithSummaryRangeValue(String field, ArrowType fieldType,
                                                                    Object lowValue, Object highValue)
    {
        Block block = Mockito.mock(Block.class);
        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(field, fieldType));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = new GcsMarker(block, Marker.Bound.ABOVE, false).withValue(lowValue);

        Marker high = new GcsMarker(block, Marker.Bound.BELOW, false).withValue(highValue);
        return Map.of(
                "salary", SortedRangeSet.of(false, new Range(low, high))
        );
    }

    // helpers
    private void addSchemaFields(SchemaBuilder schemaBuilder, boolean parquetFields)
    {
        Map<String, ArrowType> fieldMap = parquetFields ? parquetFieldMap : csvFieldMap;
        for (Map.Entry<String, ArrowType> field : fieldMap.entrySet()) {
            schemaBuilder.addField(FieldBuilder.newBuilder(field.getKey(), field.getValue()).build());
        }
    }
}
