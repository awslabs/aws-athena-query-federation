/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2023 Amazon Web Services
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

package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.MetadataVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class AthenaFederationIpcOption
{
  private static final Logger logger = LoggerFactory.getLogger(AthenaFederationIpcOption.class);

  private AthenaFederationIpcOption()
  {
  }

  public static final IpcOption DEFAULT = getIpcOption();

  private static IpcOption getIpcOption()
  {
      // This is the current way (Arrow 4.0.0+) to construct an IpcOption
      try {
          return new IpcOption(true, MetadataVersion.V4);
      }
      catch (NoSuchMethodError ex) {
          // This is expected for situations where Arrow 2.0.0 is present. Like Spark 3.2.1.
          logger.warn("Exception while constructing IpcOption (this is expected on Spark 3.2.1 and older): {}", ex.toString());
      }

      // Fall back to the Arrow 2.0.0 way of setting IpcOption
      logger.info("Falling back to old way of constructing IpcOption for Arrow 2.0.0");
      try {
          IpcOption ipcOption = new IpcOption();
          Field writeLegacyIpcFormatField = IpcOption.class.getDeclaredField("write_legacy_ipc_format");
          Field metadataVersionField = IpcOption.class.getDeclaredField("metadataVersion");
          writeLegacyIpcFormatField.setBoolean(ipcOption, true);
          metadataVersionField.set(ipcOption, MetadataVersion.V4);
          return ipcOption;
      }
      catch (ReflectiveOperationException ex) {
          // Rethrow as unchecked because most of the callers do not already declare any throws
          throw new RuntimeException(ex);
      }
  }
}
