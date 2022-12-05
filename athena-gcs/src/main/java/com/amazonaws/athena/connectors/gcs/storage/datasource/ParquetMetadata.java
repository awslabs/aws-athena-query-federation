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

import com.amazonaws.athena.connectors.gcs.GcsSchemaUtils;
import com.amazonaws.athena.connectors.gcs.storage.AbstractStorageMetadata;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;

@ThreadSafe
public class ParquetMetadata
        extends AbstractStorageMetadata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetMetadata.class);

    /**
     * This constructor, as of now, is invoked to instantiate an instance of ParquetDatasource reflectively
     *
     * @param gcsCredentialJsonString Google Cloud Storage credential JSON to access GCS
     * @param properties              Map of property/value from lambda environment
     * @throws IOException If any occurs
     */
    @SuppressWarnings("unused")
    public ParquetMetadata(String gcsCredentialJsonString,
                           Map<String, String> properties) throws IOException
    {
        this(new StorageMetadataConfig()
                .credentialsJson(gcsCredentialJsonString)
                .properties(properties));
    }

    /**
     * Instantiates a ParquetDatasource based on properties found in the GcsDatasourceConfig instance, such as
     * file_extension
     *
     * @param config An instance of GcsDatasourceConfig
     * @throws IOException If any occurs
     */
    public ParquetMetadata(StorageMetadataConfig config) throws IOException
    {
        super(config);
    }

    @Override
    public boolean isExtensionCheckMandatory()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSupported(String bucket, String objectName) throws Exception
    {
        boolean isWithValidExtension = containsInvalidExtension(objectName);
        LOGGER.debug("File {} is with valid extension? {}", objectName, isWithValidExtension);
        if (!isWithValidExtension) {
            String uri = createUri(objectName.startsWith(bucket + "/") ? objectName : bucket + "/" + objectName);
            Optional<Schema> optionalSchema = GcsSchemaUtils.getSchemaFromGcsUri(uri, getFileFormat());
            return optionalSchema.isPresent();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
<<<<<<< HEAD
    public List<StorageSplit> getSplitsByBucketPrefix(String bucket, String prefix, boolean partitioned, Constraints constraints)
    {
<<<<<<< HEAD
        LOGGER.debug("ParquetDatasource.getSplitsByBucketPrefix() -> Prefix: {} in bucket {}", prefix, bucket);
=======
        LOGGER.info("ParquetDatasource.getSplitsByBucketPrefix() -> Prefix: {} in bucket {}", prefix, bucket);
>>>>>>> 3a864c14 (Rename all instances with datasource to metadata)
        List<String> fileNames;
        if (partitioned) {
            LOGGER.debug("Location {} is a directory, walking through", prefix);
            TreeTraversalContext context = TreeTraversalContext.builder()
                    .hasParent(true)
                    .maxDepth(0)
                    .storage(storage)
                    .build();
            Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildFileOnlyTreeForPrefix(bucket,
                    bucket, prefix, context);
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
                    .build());
        }
        return splits;
    }

    /**
     * {@inheritDoc}
     */
    @Override
=======
>>>>>>> d5acf6f5 (Complete the following:)
    public FileFormat getFileFormat()
    {
        return FileFormat.PARQUET;
    }
}
