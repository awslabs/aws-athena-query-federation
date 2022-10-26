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
package com.amazonaws.athena.storage.datasource;

import com.amazonaws.athena.storage.StorageDatasource;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class FileFormat
{
    /**
     * Determines if the given extension is supported
     *
     * @param extension Extension to examine
     * @return True if the given extension is supported at this moment, false otherwise
     */
    public abstract boolean accept(String extension);

    /**
     * Creates data source reflectively based on supplied GCS authentication key JSON and Map of property/value pairs from the environment variables
     *
     * @param credentialJson Authentication Key JSON to access Google Cloud Storage
     * @param properties     Map of property/value from the environment variables
     * @return An instance of StorageDatasource based on supplied parameters
     * @throws IOException               If occurs
     * @throws NoSuchMethodException     If reflection failed
     * @throws InvocationTargetException If reflection failed
     * @throws InstantiationException    If reflection failed
     * @throws IllegalAccessException    If reflection failed
     */
    public abstract StorageDatasource createDatasource(String credentialJson,
                                                       Map<String, String> properties)
            throws IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

    /**
     * Returns name of the underlying datasource
     *
     * @return Data source name
     */
    public abstract String getDatasourceName();

    /**
     * An inner class that do the job to reflectively create a datasource of appropriate type based on supplied arguments
     */
    protected static class SupportedFileFormat extends FileFormat
    {
        private final String supportedExtension;
        private final Class<?> datasourceType;

        /**
         * Constructor that takes file extension and type for the data source
         *
         * @param extension       File extension of the data source
         * @param datasourceClass Type of the data source
         */
        protected SupportedFileFormat(String extension, Class<?> datasourceClass)
        {
            this.supportedExtension = requireNonNull(extension, "extension parameter was null in SupportedFileFormat constructor");
            this.datasourceType = requireNonNull(datasourceClass, "Datasource type not specified");
        }

        /**
         * Determines whether the underlying format supports given extension
         *
         * @param extension Extension to examine
         * @return True if the underlying format supports the given extension, false otherwise
         */
        @Override
        public boolean accept(String extension)
        {
            extension = requireNonNull(extension, "extension was null. Unable to determine whether it is supported");
            return supportedExtension.equalsIgnoreCase(extension);
        }

        /**
         * Creates the desired data source reflectively
         *
         * @param credentialJson Authentication Key JSON to access Google Cloud Storage
         * @param properties     Map of property/value from the environment variables
         * @return An instance of an appropriate StorageDatasource
         * @throws NoSuchMethodException     When reflection error occurs
         * @throws InvocationTargetException When reflection error occurs
         * @throws InstantiationException    When reflection error occurs
         * @throws IllegalAccessException    When reflection error occurs
         */
        @Override
        public StorageDatasource createDatasource(String credentialJson, Map<String, String> properties)
                throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException
        {
            Constructor<?> constructor = datasourceType.getConstructor(String.class, Map.class);
            return (StorageDatasource) constructor.newInstance(credentialJson, properties);
        }

        /**
         * Returns name of the underlying datasource
         *
         * @return Data source name
         */
        @Override
        public String getDatasourceName()
        {
            return datasourceType.getSimpleName();
        }
    }
}
