/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

public class GcsConnectorException extends RuntimeException
{
    /**
     * Constructor to instantiate with a message
     *
     * @param message Message of the Exception
     */
    public GcsConnectorException(String message)
    {
        super(message);
    }

    /**
     * Constructor to instantiate with a message and underlying instance of {@link Throwable} as a cause
     *
     * @param message Message of the Exception
     * @param error An instance of {@link Throwable} as a cause
     */
    public GcsConnectorException(String message, Throwable error)
    {
        super(message, error);
    }
}
