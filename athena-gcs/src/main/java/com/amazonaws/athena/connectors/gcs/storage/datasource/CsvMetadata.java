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

import com.amazonaws.athena.connectors.gcs.storage.AbstractStorageMetadata;
import org.apache.arrow.dataset.file.FileFormat;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Map;

@ThreadSafe
public class CsvMetadata
        extends AbstractStorageMetadata
{
    // Used by reflection
    @SuppressWarnings("unused")
    public CsvMetadata(String storageCredentialJsonString,
                       Map<String, String> properties) throws IOException
    {
        this(new StorageMetadataConfig()
                .credentialsJson(storageCredentialJsonString)
                .properties(properties));
    }

    public CsvMetadata(StorageMetadataConfig config) throws IOException
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
    public boolean isSupported(String bucket, String objectName)
    {
        return objectName.toLowerCase().endsWith(metadataConfig.extension());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileFormat getFileFormat()
    {
        return FileFormat.CSV;
    }
}