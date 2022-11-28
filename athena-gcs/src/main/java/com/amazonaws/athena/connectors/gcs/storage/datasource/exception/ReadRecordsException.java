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
package com.amazonaws.athena.connectors.gcs.storage.datasource.exception;

public class ReadRecordsException extends UncheckedStorageDatasourceException
{
    /**
     * Instantiate the exception with a message and Throwable instance
     *
     * @param message   Message of the exception
     * @param throwable Base exception for tracing
     */
    public ReadRecordsException(String message, Throwable throwable)
    {
        super(message, throwable);
    }
}
