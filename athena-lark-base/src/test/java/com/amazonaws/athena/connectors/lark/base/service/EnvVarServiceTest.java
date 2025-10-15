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
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.amazonaws.athena.connectors.lark.base.model.SecretValue;
import com.amazonaws.athena.connectors.lark.base.throttling.BaseExceptionFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class EnvVarServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void constructor_success() throws Exception {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(LARK_APP_KEY_ENV_VAR, "test_secret");

        SecretValue secretValue = new SecretValue("test_app_id", "test_app_secret");
        String secretJson = objectMapper.writeValueAsString(secretValue);

        ThrottlingInvoker invoker = Mockito.mock(ThrottlingInvoker.class);
        when(invoker.invoke(any())).thenReturn(secretJson);

        EnvVarService envVarService = new EnvVarService(configOptions, invoker);

        assertEquals("test_app_id", envVarService.getLarkAppId());
        assertEquals("test_app_secret", envVarService.getLarkAppSecret());
    }

    @Test
    public void constructor_missingSecretName() {
        Map<String, String> configOptions = new HashMap<>();
        ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(new BaseExceptionFilter(), configOptions).build();

        assertThrows(IllegalArgumentException.class, () -> new EnvVarService(configOptions, invoker));
    }

    @Test
    public void constructor_secretManagerTimeout() throws Exception {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(LARK_APP_KEY_ENV_VAR, "test_secret");

        ThrottlingInvoker invoker = Mockito.mock(ThrottlingInvoker.class);
        when(invoker.invoke(any())).thenThrow(new TimeoutException());

        assertThrows(IllegalArgumentException.class, () -> new EnvVarService(configOptions, invoker));
    }

    @Test
    public void constructor_invalidJson() throws Exception {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(LARK_APP_KEY_ENV_VAR, "test_secret");

        ThrottlingInvoker invoker = Mockito.mock(ThrottlingInvoker.class);
        when(invoker.invoke(any())).thenReturn("invalid_json");

        assertThrows(IllegalArgumentException.class, () -> new EnvVarService(configOptions, invoker));
    }

    @Test
    public void getters_success() throws Exception {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(LARK_APP_KEY_ENV_VAR, "test_secret");
        configOptions.put(DOES_ACTIVATE_EXPERIMENTAL_FEATURE_ENV_VAR, "true");
        configOptions.put(DOES_ACTIVATE_LARK_BASE_SOURCE_ENV_VAR, "true");
        configOptions.put(DOES_ACTIVATE_LARK_DRIVE_SOURCE_ENV_VAR, "true");
        configOptions.put(DOES_ACTIVATE_PARALLEL_SPLIT_ENV_VAR, "true");
        configOptions.put(ENABLE_DEBUG_LOGGING_ENV_VAR, "true");
        configOptions.put(LARK_BASE_SOURCES_ENV_VAR, "base1,base2");
        configOptions.put(LARK_DRIVE_SOURCES_ENV_VAR, "drive1,drive2");

        SecretValue secretValue = new SecretValue("test_app_id", "test_app_secret");
        String secretJson = objectMapper.writeValueAsString(secretValue);

        ThrottlingInvoker invoker = Mockito.mock(ThrottlingInvoker.class);
        when(invoker.invoke(any())).thenReturn(secretJson);

        EnvVarService envVarService = new EnvVarService(configOptions, invoker);

        assertTrue(envVarService.isActivateExperimentalFeatures());
        assertTrue(envVarService.isActivateLarkBaseSource());
        assertTrue(envVarService.isActivateLarkDriveSource());
        assertTrue(envVarService.isActivateParallelSplit());
        assertTrue(envVarService.isEnableDebugLogging());
        assertEquals("base1,base2", envVarService.getLarkBaseSources());
        assertEquals("drive1,drive2", envVarService.getLarkDriveSources());
    }
}
