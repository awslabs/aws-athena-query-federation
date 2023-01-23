package com.amazonaws.athena.connector.lambda.handlers;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

/**
 * Used to convey the version of serialization of this SDK instance when negotiating functionality with
 * Athena. You can think of this like a version number that is specific to the protocol used by the SDK.
 * Any modification in the way existing over-the-wire objects are serialized would require incrementing
 * this value.
 */
public class SerDeVersion
{
    private SerDeVersion() {}

    public static final int SERDE_VERSION = 4;
}
