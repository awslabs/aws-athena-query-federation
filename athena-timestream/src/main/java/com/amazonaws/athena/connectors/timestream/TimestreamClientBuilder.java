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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestreamClientBuilder
{
  private static final Logger logger = LoggerFactory.getLogger(TimestreamClientBuilder.class);

  private TimestreamClientBuilder()
  {
    // prevent instantiation with private constructor
  }

  public static AmazonTimestreamQuery buildQueryClient(String sourceType)
  {
    return AmazonTimestreamQueryClientBuilder.standard().withClientConfiguration(buildClientConfiguration(sourceType)).build();
  }

  public static AmazonTimestreamWrite buildWriteClient(String sourceType)
  {
    return AmazonTimestreamWriteClientBuilder.standard().withClientConfiguration(buildClientConfiguration(sourceType)).build();
  }

  static ClientConfiguration buildClientConfiguration(String sourceType)
  {
    String userAgent = "aws-athena-" + sourceType + "-connector";
    ClientConfiguration clientConfiguration = new ClientConfiguration().withUserAgentPrefix(userAgent);
    logger.info("Created client configuration with user agent {} for Timestream SDK", clientConfiguration.getUserAgentPrefix());
    return clientConfiguration;
  }
}
