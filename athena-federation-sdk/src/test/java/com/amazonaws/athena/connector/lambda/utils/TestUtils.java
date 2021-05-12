/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;

public class TestUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    public final static int SERDE_VERSION_ONE = 1;
    public final static int SERDE_VERSION_TWO = 2;
    public final static int SERDE_VERSION_THREE = 3;

    /**
     * Helper to retrieve resources from the class path and enforce they are found
     * @param locationHint Used to help find the resources if a classpath scan isn't working
     * @param resource The name of the file (resource) to find on the classpath
     * @return The path to the resource
     *
     * @note Throws a runtime exception if the resources is not found
     */
    public String getResourceOrFail(String locationHint, String resource) {
        String resourcePath = (locationHint != null) ? locationHint + "/" : "";
        resourcePath += resource;
        URL resourceUrl = this.getClass().getClassLoader().getResource(resourcePath);

        if (resourceUrl == null) {
            //fallback to looking for this in our test-resources-dir
            String path = System.getProperty("test-resources-dir");
            String buildPath = (locationHint != null) ? locationHint + "/" : "";
            buildPath += resource;
            File resourceFile = new File(path + "/" + buildPath);
            if (resourceFile.exists()) {
                return resourceFile.getPath();
            }
        }

        requireNonNull(resourceUrl, "Unable to find resource[" + resource + "]");
        LOGGER.info("getResourceOrFail: found[" + resource + "] at [" + resourceUrl + "]");
        return resourceUrl.getFile();
    }

    /**
     * Helper to retrieve and read resources from the class path which enforces that they are found
     * @param locationHint Used to help find the resources if a classpath scan isn't working
     * @param resource The name of the file (resource) to find on the classpath
     * @return The string content of the file found in default encoding.
     *
     * @note Throws a runtime exception if the resources is not found
     */
    public String readFileAsString(String locationHint, String resource) throws IOException
    {
        String file = getResourceOrFail(locationHint, resource);
        return readAllAsString(file);
    }

    /**
     * Used to read an entire file as a String.
     * @param file The file path to read.
     * @return String containing the entire file contents.
     * @throws IOException
     */
    public String readAllAsString(String file) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(file));
        return new String(encoded);
    }

    /**
     * Used to read an entire file as a String.
     *
     * @param file The file path to read.
     * @return String containing the entire file contents.
     */
    public String readAllAsString(String file, Charset charset) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(file));
        return new String(encoded, charset);
    }
}
