/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.resolver;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;

public abstract class CaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CaseResolver.class);
    public static final String CASING_MODE_CONFIGURATION_KEY = "casing_mode";

    protected String sourceType;
    protected FederationSDKCasingMode nonGlueBasedDefaultCasingMode;
    protected FederationSDKCasingMode glueConnectionBasedDefaultCasingMode;

    public CaseResolver(String sourceType, FederationSDKCasingMode nonGlueBasedDefaultCasingMode, FederationSDKCasingMode glueConnectionBasedDefaultCasingMode)
    {
        this.sourceType = sourceType;
        this.nonGlueBasedDefaultCasingMode = nonGlueBasedDefaultCasingMode;
        this.glueConnectionBasedDefaultCasingMode = glueConnectionBasedDefaultCasingMode;
    }

    public enum FederationSDKCasingMode
    {
        NONE,
        LOWER,      // casing mode to lower case everything (glue and trino lower case everything)
        UPPER,      // casing mode to upper case everything (oracle by default upper cases everything)
        CASE_INSENSITIVE_SEARCH, //
        ANNOTATION
    }

    protected final FederationSDKCasingMode getCasingMode(Map<String, String> configOptions)
    {
        boolean isGlueConnection = StringUtils.isNotBlank(configOptions.get(DEFAULT_GLUE_CONNECTION));
        if (!configOptions.containsKey(CASING_MODE_CONFIGURATION_KEY)) {
            FederationSDKCasingMode casingMode = isGlueConnection ? glueConnectionBasedDefaultCasingMode : nonGlueBasedDefaultCasingMode;
            LOGGER.debug("CASING MODE disable default casing mode for :'{}' is :'{}'", sourceType, casingMode);
            return casingMode;
        }

        try {
            FederationSDKCasingMode casingModeFromConfig = FederationSDKCasingMode.valueOf(configOptions.get(CASING_MODE_CONFIGURATION_KEY).toUpperCase());
            LOGGER.debug("CASING MODE enable: {}", casingModeFromConfig.toString());
            return casingModeFromConfig;
        }
        catch (Exception ex) {
            // print error log for customer along with list of input
            LOGGER.error("Invalid input for:{}, source type:{}, input value:{}, valid values:{}", CASING_MODE_CONFIGURATION_KEY, sourceType, configOptions.get(CASING_MODE_CONFIGURATION_KEY),
                    Arrays.asList(FederationSDKCasingMode.values()), ex);
            throw ex;
        }
    }
}
