/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs.filter;

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
import com.amazonaws.athena.connectors.gcs.GcsTestUtils;
import com.amazonaws.athena.connectors.gcs.storage.StorageConstants;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.checkerframework.checker.nullness.qual.AssertNonNullIfNonNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class})
public class FilterExpressionBuilderTest
{
    private static final String STORAGE_SPLIT_JSON = "storage_split_json";
    @Mock
    public FederatedIdentity federatedIdentity;

    @Test
    public void testGetExpressions() throws Exception
    {
        StorageSplit storageSplit = StorageSplit.builder().fileName("athena-30part-nested/data/year=2000/Month_col=5/data.parquet").build();
        Schema schema = SchemaBuilder.newBuilder().addField("id", new ArrowType.Int(64, false)).build();
        FilterExpressionBuilder filterExpressionBuilder = new FilterExpressionBuilder(schema);
        AthenaReadRecordsRequest request = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("id", new ArrowType.Int(64, false), 1L), "BUCKET", "PARQUET_TABLE", storageSplit, true);
        List<FilterExpression> exp = filterExpressionBuilder.getExpressions(request.getConstraints());
        assertNotNull(exp);
    }

    protected Map<String, ValueSet> createSummaryWithLValueRangeEqual(String fieldName, ArrowType fieldType, Object fieldValue)
    {
        Block block = Mockito.mock(Block.class);
        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(fieldName, fieldType));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = new AthenaMarker(block, Marker.Bound.EXACTLY, false).withValue(fieldValue);
        return Map.of(
                fieldName, SortedRangeSet.of(false, new Range(low, low))
        );
    }


    public synchronized AthenaReadRecordsRequest buildReadRecordsRequest(Map<String, ValueSet> summary,
                                                                         String schema, String table, StorageSplit split,
                                                                         boolean parquetFields) throws JsonProcessingException
    {
        TableName inputTableName = new TableName(schema, table);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        addSchemaFields(schemaBuilder, parquetFields);
        Schema fieldSchema = schemaBuilder.build();
        Constraints constraints = new AthenaConstraints(summary);
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add(StorageConstants.STORAGE_SPLIT_JSON, new ObjectMapper().writeValueAsString(split))
                .add(TABLE_PARAM_OBJECT_NAME_LIST, split.getFileName());
        return new AthenaReadRecordsRequest(this.federatedIdentity,
                "default", "testQueryId", inputTableName,
                fieldSchema, splitBuilder.build(), constraints, 1024, 1024);
    }

    // helpers
    private void addSchemaFields(SchemaBuilder schemaBuilder, boolean parquetFields)
    {
        Map<String, ArrowType> fieldMap = parquetFieldMap ;
        for (Map.Entry<String, ArrowType> field : fieldMap.entrySet()) {
            schemaBuilder.addField(FieldBuilder.newBuilder(field.getKey(), field.getValue()).build());
        }
    }

    public final Map<String, ArrowType> parquetFieldMap = Map.of(
            "id", BIGINT.getType()
    );

}
