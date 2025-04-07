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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CaseResolverTest {

    private CaseResolver caseResolver;

    private static final String DEFAULT_GLUE_CONNECTION = "default_glue_connection";

    private static class TestCaseResolver extends CaseResolver {
        public TestCaseResolver(String sourceType, FederationSDKCasingMode nonGlueBasedDefaultCasingMode, FederationSDKCasingMode glueConnectionBasedDefaultCasingMode) {
            super(sourceType, nonGlueBasedDefaultCasingMode, glueConnectionBasedDefaultCasingMode);
        }
    }

    @Test
    void testGetCasingMode_NoConfigKey_DefaultsToNonGlueBased() {
        caseResolver = new TestCaseResolver("TestSource", CaseResolver.FederationSDKCasingMode.LOWER, CaseResolver.FederationSDKCasingMode.UPPER);
        Map<String, String> configOptions = new HashMap<>();
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.LOWER, result);
    }

    @Test
    void testGetCasingMode_WithGlueConnection_DefaultsToGlueBased() {
        caseResolver = new TestCaseResolver("TestSource", CaseResolver.FederationSDKCasingMode.LOWER, CaseResolver.FederationSDKCasingMode.UPPER);
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(DEFAULT_GLUE_CONNECTION, "true");
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.LOWER, result);
    }

    @Test
    void testGetCasingMode_ValidConfig() {
        caseResolver = new TestCaseResolver("TestSource", CaseResolver.FederationSDKCasingMode.LOWER, CaseResolver.FederationSDKCasingMode.UPPER);
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(CaseResolver.CASING_MODE_CONFIGURATION_KEY, "ANNOTATION");
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.ANNOTATION, result);
    }

    @Test
    void testGetCasingMode_InvalidConfig() {
        caseResolver = new TestCaseResolver("TestSource", CaseResolver.FederationSDKCasingMode.LOWER, CaseResolver.FederationSDKCasingMode.UPPER);
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(CaseResolver.CASING_MODE_CONFIGURATION_KEY, "INVALID_MODE");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            caseResolver.getCasingMode(configOptions);
        });

        assertTrue(exception.getMessage().contains("INVALID_MODE"));
    }
}
