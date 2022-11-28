/*-
 * #%L
 * athena-hive
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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.athena.connectors.gcs.storage.datasource;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.common.StorageNode;
import com.amazonaws.athena.connectors.gcs.common.StorageTreeNodeBuilder;
import com.amazonaws.athena.connectors.gcs.common.TreeTraversalContext;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.storage.AbstractStorageDatasource;
import com.amazonaws.athena.connectors.gcs.storage.GroupSplit;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.getRootName;

@ThreadSafe
public class CsvDatasource
        extends AbstractStorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvDatasource.class);

    // Used by reflection
    @SuppressWarnings("unused")
    public CsvDatasource(String storageCredentialJsonString,
                         Map<String, String> properties) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this(new StorageDatasourceConfig()
                .credentialsJson(storageCredentialJsonString)
                .properties(properties));
    }

    public CsvDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        super(config);
    }
    @Override
    public boolean isExtensionCheckMandatory()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FilterExpression> getExpressions(String bucket, String objectName, Schema schema, TableName tableName, Constraints constraints,
                                                 Map<String, String> partitionFieldValueMap)
    {
        return List.of();
    }

    @Override
    public boolean isSupported(String bucket, String objectName)
    {
        return objectName.toLowerCase().endsWith(datasourceConfig.extension());
    }
    @Override
    public List<StorageSplit> getSplitsByBucketPrefix(String bucket, String prefix, boolean partitioned, Constraints constraints) throws IOException
    {
        LOGGER.info("ParquetDatasource.getSplitsByBucketPrefix() -> Prefix: {} in bucket {}", prefix, bucket);
        List<String> fileNames;
        if (partitioned) {
            LOGGER.debug("Location {} is a directory, walking through", prefix);
            TreeTraversalContext context = TreeTraversalContext.builder()
                    .hasParent(true)
                    .maxDepth(0)
                    .storage(storage)
                    .build();
            Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildFileOnlyTreeForPrefix(bucket,
                    getRootName(prefix), prefix, context);
            if (optionalRoot.isPresent()) {
                fileNames = optionalRoot.get().getChildren().stream()
                        .map(StorageNode::getPath)
                        .collect(Collectors.toList());
            }
            else {
                LOGGER.debug("Prefix {}'s root  not present", prefix);
                return List.of();
            }
        }
        else {
            fileNames = List.of(prefix);
        }
        List<StorageSplit> splits = new ArrayList<>();
        LOGGER.debug("Splitting based on files {}", prefix);
        for (String fileName : fileNames) {
            splits.add(StorageSplit.builder()
                    .fileName(fileName)
                    .groupSplits(List.of(GroupSplit.builder()
                            .groupIndex(0)
                            .rowOffset(0)
                            .rowCount(Integer.MAX_VALUE)
                            .build()))
                    .build());
        }
        return splits;
    }

    @Override
    public FileFormat getFileFormat()
    {
        return FileFormat.CSV;
    }

    @Override
    public int recordsPerSplit()
    {
        return 10_000;
    }
}
