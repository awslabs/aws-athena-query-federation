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

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;

public class StorageMetadataConfig
{
    /**
     * GCP Google cloud storage application credential retrieved from the SecretsManager
     */
    private String storageCredentialJson;

    /**
     * Properties retrieved from System environment varaibles
     */
    private final Map<String, String> properties = new HashMap<>();

    public Map<String, String> properties()
    {
        return new HashMap<>(properties);
    }

    public String credentialsJson()
    {
        return storageCredentialJson;
    }

    /**
     * Fluent-style setter that sets the Google Cloud Storage auth JSON for access
     *
     * @param json Google Cloud Storage auth JSON string
     * @return Return the instance of GcsDatasourceConfig upon which this setter is invoked
     */
    public StorageMetadataConfig credentialsJson(String json)
    {
        storageCredentialJson = json;
        return this;
    }

    /**
     * Fluent-style setter that sets the Map of property/value paris from System.env (Usually from lambda environment variables)
     *
     * @param properties Map of property/value paris from System.env (Usually from lambda environment variables)
     * @return Return the instance of GcsDatasourceConfig upon which this setter is invoked
     */
    public StorageMetadataConfig properties(Map<String, String> properties)
    {
        this.properties.putAll(properties);
        return this;
    }

    /**
     * Returns the file extension set via the file_extension environment variable
     *
     * @return File extension
     */
    public String extension()
    {
        return "." + properties.get(FILE_EXTENSION_ENV_VAR).toLowerCase();
    }

    public String getPropertyElseDefault(String key, String defaultValue)
    {
        String val = properties.get(key);
        return  (val == null || val.isBlank())
                ? defaultValue
                : val;
    }
}
