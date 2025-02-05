
/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2020 Amazon web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link ImpalaMetadataHandler} and {@link ImpalaRecordHandler}.
 *
 * Recommend using {@link ImpalaMuxCompositeHandler} instead.
 */
public class ImpalaCompositeHandler
        extends CompositeHandler
{
    public ImpalaCompositeHandler()
    {
        super(new ImpalaMetadataHandler(new ImpalaEnvironmentProperties().createEnvironment()),
                new ImpalaRecordHandler(new ImpalaEnvironmentProperties().createEnvironment()));
    }
}
