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
package com.amazonaws.athena.connectors.gcs.storage;

import static java.util.Objects.requireNonNull;

public class StorageSplit
{
    private String fileName;

    // Jackson uses this constructor
    @SuppressWarnings("unused")
    public StorageSplit()
    {
    }

    /**
     * Constructor to instantiate with the given arguments
     *
     * @param fileName    Name of the file from GCS
     */
    public StorageSplit(String fileName)
    {
        this.fileName = requireNonNull(fileName, "File name can't be null");
    }

    public String getFileName()
    {
        return fileName;
    }

    @Override
    public String toString()
    {
        return "StorageSplit("
                + "fileName=" + fileName + ","
                + ")";
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Fluent-styled convenience builder class to instantiate a {@link StorageSplit} instance
     */
    public static class Builder
    {
        private Builder()
        {
        }

        private String fileName;

        public Builder fileName(String fileName)
        {
            this.fileName = fileName;
            return this;
        }

        /**
         * Creates an instance of {@link StorageSplit}. It checks to see if the minimum required parameters exists (not null)
         *
         * @return An instance of {@link StorageSplit}
         */
        public StorageSplit build()
        {
            return new StorageSplit(this.fileName);
        }
    }
}
