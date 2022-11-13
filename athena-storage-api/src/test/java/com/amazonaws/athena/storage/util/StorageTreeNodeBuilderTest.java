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
package com.amazonaws.athena.storage.util;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.common.StorageNode;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.TreeTraversalContext;
import com.amazonaws.athena.storage.datasource.parquet.filter.EqualsExpression;
import com.amazonaws.athena.storage.mock.GcsConstraints;
import com.amazonaws.athena.storage.mock.GcsMarker;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT4;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

public class StorageTreeNodeBuilderTest extends GcsTestBase {

    private final static String BUCKET = "mydatalake1";

    private final Map<String, ArrowType> parquetFieldSchemaMap = Map.of(
            "statename", VARCHAR.getType(),
            "countryname", VARCHAR.getType(),
            "zipcode", BIGINT.getType()
    );

    @Test
    public void testNestedPartitionedFolderWithoutFilter() throws Exception {
        TreeTraversalContext context = TreeTraversalContext.builder()
                .hasParent(true)
                .includeFile(false)
                .maxDepth(0)
                .partitionDepth(1)
                .storageDatasource(getTestDataSource("parquet"))
                .build();
        Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildTreeWithPartitionedDirectories(BUCKET,
                "zipcode", "zipcode", context);
        assertTrue("Partitioned folder note found with filter", optionalRoot.isPresent());
        System.out.println(optionalRoot.get().getChildren());
    }

    @Test
    public void testNestedPartitionedFolderWithFilter() throws Exception {
        TreeTraversalContext context = TreeTraversalContext.builder()
                .hasParent(true)
                .includeFile(false)
                .maxDepth(0)
                .partitionDepth(1)
                .storageDatasource(getTestDataSource("parquet"))
                .build();
        context.addAllFilers(List.of(
                new EqualsExpression(1, "statename", "UP")
        ));
        Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildTreeWithPartitionedDirectories(BUCKET,
                "zipcode", "zipcode", context);
        assertTrue("Partitioned folder note found with filter", optionalRoot.isPresent());
        System.out.println(optionalRoot.get().getChildren());
    }

    @Test
    public void testGetPartitionWithTreeNodeBuilder() throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        addSchemaFields(schemaBuilder, true);
        Schema fieldSchema = schemaBuilder.build();
        Constraints constraints = new GcsConstraints(createSummary());
        TableName tableName = new TableName("mydatalake1", "zipcode");
        StorageDatasource datasource = getTestDataSource("parquet");
        List<StoragePartition> partitions = datasource.getStoragePartitions(fieldSchema, tableName, constraints, BUCKET, "zipcode/");
        assertFalse("No partitions found", partitions.isEmpty());
        System.out.println(partitions);
    }

    @Test
    public void testNestedFilesOnlyByPrefix() throws Exception {
        TreeTraversalContext context = TreeTraversalContext.builder()
                .hasParent(true)
                .maxDepth(0)
                .storageDatasource(getTestDataSource("parquet"))
                .build();
        Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildFileOnlyTreeForPrefix(BUCKET,
                "zipcode", "zipcode/StateName='UP'/", context);
        assertTrue("File(s) not found with prefix zipcode/StateName='UP'/" , optionalRoot.isPresent());
        System.out.println(optionalRoot.get().getChildren());
    }


    private void addSchemaFields(SchemaBuilder schemaBuilder, boolean parquetFields)
    {
        Map<String, ArrowType> fieldMap = parquetFields ? parquetFieldSchemaMap : csvFieldMap;
        for (Map.Entry<String, ArrowType> field : fieldMap.entrySet()) {
            schemaBuilder.addField(FieldBuilder.newBuilder(field.getKey(), field.getValue()).build());
        }
    }

    public Map<String, ValueSet> createSummary()
    {
        Block block = Mockito.mock(Block.class);
        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable("statename", Types.MinorType.VARCHAR.getType()));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = new GcsMarker(block, Marker.Bound.EXACTLY, false).withValue("UP");
        return Map.of(
                "statename", SortedRangeSet.of(false, new Range(low, low))
        );
    }
}
