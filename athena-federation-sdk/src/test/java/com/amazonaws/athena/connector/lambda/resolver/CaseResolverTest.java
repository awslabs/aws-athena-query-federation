package com.amazonaws.athena.connector.lambda.resolver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

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

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        caseResolver = new TestCaseResolver("TestSource", CaseResolver.FederationSDKCasingMode.LOWER, CaseResolver.FederationSDKCasingMode.UPPER);
    }

    @Test
    void testGetCasingMode_NoConfigKey_DefaultsToNonGlueBased() {
        Map<String, String> configOptions = new HashMap<>();
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.LOWER, result);
    }

    @Test
    void testGetCasingMode_WithGlueConnection_DefaultsToGlueBased() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(DEFAULT_GLUE_CONNECTION, "true");
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.LOWER, result);
    }

    @Test
    void testGetCasingMode_ValidConfig() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(CaseResolver.CASING_MODE_CONFIGURATION_KEY, "ANNOTATION");
        CaseResolver.FederationSDKCasingMode result = caseResolver.getCasingMode(configOptions);
        assertEquals(CaseResolver.FederationSDKCasingMode.ANNOTATION, result);
    }

    @Test
    void testGetCasingMode_InvalidConfig() {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(CaseResolver.CASING_MODE_CONFIGURATION_KEY, "INVALID_MODE");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            caseResolver.getCasingMode(configOptions);
        });

        assertTrue(exception.getMessage().contains("INVALID_MODE"));
    }
}
