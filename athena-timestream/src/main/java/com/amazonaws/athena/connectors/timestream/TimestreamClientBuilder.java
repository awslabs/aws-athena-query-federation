/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

public class TimestreamClientBuilder
{
  private static final Logger logger = LoggerFactory.getLogger(TimestreamClientBuilder.class);
  static Region defaultRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();
  private TimestreamClientBuilder()
  {
    // prevent instantiation with private constructor
  }

  public static TimestreamQueryClient buildQueryClient(String sourceType)
  {
    return TimestreamQueryClient.builder().region(defaultRegion).credentialsProvider(DefaultCredentialsProvider.create())
            .overrideConfiguration(buildClientConfiguration(sourceType)).build();
  }

  public static TimestreamWriteClient buildWriteClient(String sourceType)
  {
    return TimestreamWriteClient.builder().region(defaultRegion).credentialsProvider(DefaultCredentialsProvider.create())
            .overrideConfiguration(buildClientConfiguration(sourceType)).build();
  }

  static ClientOverrideConfiguration buildClientConfiguration(String sourceType)
  {
    String userAgent = "aws-athena-" + sourceType + "-connector";
    ClientOverrideConfiguration clientConfiguration = ClientOverrideConfiguration.builder().putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, userAgent).build();
    logger.info("Created client configuration with user agent {} for Timestream SDK is present", clientConfiguration.advancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX).isPresent());
    return clientConfiguration;
  }
}
