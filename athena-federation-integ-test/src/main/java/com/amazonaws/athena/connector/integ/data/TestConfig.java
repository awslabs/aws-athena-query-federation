/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.integ.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class is responsible for extracting the attributes from the test config file (test-config.json), and provides
 * simple extractors for the attributes within.
 */
public class TestConfig
{
    private static final String TEST_CONFIG_FILE_NAME = "etc/test-config.json";

    private final Map<String, Object> config;

    /**
     * The constructor loads the test configuration attributes from a file into a config Map.
     */
    public TestConfig()
    {
        config = setUpTestConfig();
    }

    /**
     * Loads the test configuration attributes from a file into a Map.
     * @return Map(String, Object) containing the test configuration attributes.
     * @throws RuntimeException Error encountered while trying to access or parse the config file.
     */
    private Map<String, Object> setUpTestConfig()
    {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            return objectMapper.readValue(new File(TEST_CONFIG_FILE_NAME), HashMap.class);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to parse test-config.json: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a single item from the config file.
     * @param attribute The name of the item being extracted from the config file.
     * @return Optional Object, or empty Optional if the retrieval of the attribute results in a null value.
     */
    public Optional<Object> getItem(String attribute)
    {
        return Optional.ofNullable(config.get(attribute));
    }

    /**
     * Gets a String item from the config file.
     * @param attribute The name of the item being extracted from the config file.
     * @return An Optional String, or empty Optional if the attribute is either an empty String or not a String.
     * @throws RuntimeException The attribute does not exist in the config file.
     */
    public Optional<String> getStringItem(String attribute)
            throws RuntimeException
    {
        Object item = getItem(attribute).orElseThrow(() ->
                new RuntimeException(attribute + " does not exist test-config.json file."));

        if (!(item instanceof String) || ((String) item).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((String) item);
    }

    /**
     * Gets a map item from the config file.
     * @param attribute The name of the item being extracted from the config file.
     * @return Optional Map(String, Object) that can be further parsed, or an empty Optional if the map is empty or
     * not a Map.
     * @throws RuntimeException The attribute does not exist in the config file.
     */
    public Optional<Map<String, Object>> getMap(String attribute)
            throws RuntimeException
    {
        Object item = getItem(attribute).orElseThrow(() ->
                new RuntimeException(attribute + " does not exist test-config.json file."));

        if (!(item instanceof Map) || ((Map) item).isEmpty()) {
            return Optional.empty();
        }

        return Optional.of((Map) item);
    }

    /**
     * Gets a string map item from the config file.
     * @param attribute The name of the item being extracted from the config file.
     * @return Optional Map(String, String), or an empty Optional if the map is empty or not a Map.
     * @throws RuntimeException The attribute does not exist in the config file.
     */
    public Optional<Map<String, String>> getStringMap(String attribute)
            throws RuntimeException
    {
        Map<String, String> mapOfStrings = new HashMap<>();

        Object item = getItem(attribute).orElseThrow(() ->
                new RuntimeException(attribute + " does not exist test-config.json file."));

        if (item instanceof Map) {
            ((Map) item).forEach((key, value) -> {
                if ((key instanceof String) && (value instanceof String)) {
                    mapOfStrings.put((String) key, (String) value);
                }
            });
        }

        return mapOfStrings.isEmpty() ? Optional.empty() : Optional.of(mapOfStrings);
    }
}
