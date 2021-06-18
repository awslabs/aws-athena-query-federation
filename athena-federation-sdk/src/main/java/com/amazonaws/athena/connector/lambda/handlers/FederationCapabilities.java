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
 * Used to convey the capabilities of this SDK instance when negotiating functionality with
 * Athena. You can think of this like a version number that is specific to the feature set
 * and protocol used by the SDK. Purely client side changes in the SDK would not be expected
 * to change the capabilities.
 *
 * Version history:
 * 23 - initial preview release
 * 24 - explicit, versioned serialization introduced
 * 25 - upgraded Arrow to 3.0.0, addressed backwards incompatible changes
 */
public class FederationCapabilities
{
    private FederationCapabilities() {}

    protected static final int CAPABILITIES = 24;
}
