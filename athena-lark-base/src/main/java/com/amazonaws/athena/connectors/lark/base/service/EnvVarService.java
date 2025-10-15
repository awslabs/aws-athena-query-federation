/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.service;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.lark.base.model.SecretValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.DOES_ACTIVATE_EXPERIMENTAL_FEATURE_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.DOES_ACTIVATE_LARK_BASE_SOURCE_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.DOES_ACTIVATE_LARK_DRIVE_SOURCE_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.DOES_ACTIVATE_PARALLEL_SPLIT_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.ENABLE_DEBUG_LOGGING_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_APP_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_BASE_SOURCES_ENV_VAR;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_DRIVE_SOURCES_ENV_VAR;
import static java.util.Objects.requireNonNull;

public class EnvVarService
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final CachableSecretsManager secretsManagerClient;
    private final String larkAppId;
    private final String larkAppSecret;
    private final boolean activateExperimentalFeatures;
    private final boolean activateLarkBaseSource;
    private final boolean activateLarkDriveSource;
    private final boolean activateParallelSplit;
    private final boolean enableDebugLogging;
    private final String larkBaseSources;
    private final String larkDriveSources;

    public EnvVarService(Map<String, String> configOptions, ThrottlingInvoker invoker)
    {
        requireNonNull(configOptions, "configOptions is null");

        this.secretsManagerClient = new CachableSecretsManager(SecretsManagerClient.create());

        String secretName = configOptions.get(LARK_APP_KEY_ENV_VAR);
        if (secretName == null || secretName.isEmpty()) {
            throw new IllegalArgumentException("Environment variable " + LARK_APP_KEY_ENV_VAR + " is not set.");
        }

        String rawLarkApp;
        try {
            rawLarkApp = invoker.invoke(() -> (secretsManagerClient.getSecret(secretName)));
        }
        catch (TimeoutException e) {
            throw new IllegalArgumentException("Environment variable " + LARK_APP_KEY_ENV_VAR + " is not set.", e);
        }

        SecretValue secretMapping;
        try {
            secretMapping = objectMapper.readValue(rawLarkApp, SecretValue.class);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Environment variable " + LARK_APP_KEY_ENV_VAR + " is not set.", e);
        }

        this.larkAppId = secretMapping.larkAppId();
        this.larkAppSecret = secretMapping.larkAppSecret();

        this.activateExperimentalFeatures = Boolean.parseBoolean(configOptions.getOrDefault(DOES_ACTIVATE_EXPERIMENTAL_FEATURE_ENV_VAR, "false"));
        this.activateLarkBaseSource = Boolean.parseBoolean(configOptions.getOrDefault(DOES_ACTIVATE_LARK_BASE_SOURCE_ENV_VAR, "false"));
        this.activateLarkDriveSource = Boolean.parseBoolean(configOptions.getOrDefault(DOES_ACTIVATE_LARK_DRIVE_SOURCE_ENV_VAR, "false"));
        this.activateParallelSplit = Boolean.parseBoolean(configOptions.getOrDefault(DOES_ACTIVATE_PARALLEL_SPLIT_ENV_VAR, "false"));
        this.enableDebugLogging = Boolean.parseBoolean(configOptions.getOrDefault(ENABLE_DEBUG_LOGGING_ENV_VAR, "false"));
        this.larkBaseSources = configOptions.getOrDefault(LARK_BASE_SOURCES_ENV_VAR, "");
        this.larkDriveSources = configOptions.getOrDefault(LARK_DRIVE_SOURCES_ENV_VAR, "");
    }

    public String getLarkAppId()
    {
        return larkAppId;
    }

    public String getLarkAppSecret()
    {
        return larkAppSecret;
    }

    public boolean isActivateExperimentalFeatures()
    {
        return activateExperimentalFeatures;
    }

    public boolean isActivateLarkBaseSource()
    {
        return activateLarkBaseSource;
    }

    public boolean isActivateLarkDriveSource()
    {
        return activateLarkDriveSource;
    }

    public boolean isActivateParallelSplit()
    {
        return activateParallelSplit;
    }

    public boolean isEnableDebugLogging()
    {
        return enableDebugLogging;
    }

    public String getLarkBaseSources()
    {
        return larkBaseSources;
    }

    public String getLarkDriveSources()
    {
        return larkDriveSources;
    }
}
