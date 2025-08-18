/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SSL_CERT_FILE_LOCATION_VALUE;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnowflakeUtilsTest
{
    static {
        System.setProperty("line.separator", "\n");
    }
    @TempDir
    Path tempDir;  // Creates a temporary directory for test files

    private String testCertFilePath;

    @BeforeEach
    void setUp() {
        testCertFilePath = tempDir.resolve("test_cacerts.pem").toString();
    }

    @AfterAll
    static void cleanup() {
        File certFile = new File(SSL_CERT_FILE_LOCATION_VALUE);
        certFile.deleteOnExit();
    }

    @Test
    void testInstallCaCertificate() throws Exception {
            // Run method
            SnowflakeUtils.installCaCertificate();

            // Verify file was created
            File certFile = new File(SSL_CERT_FILE_LOCATION_VALUE);
            assertTrue(certFile.exists(), "Certificate file should be created.");
    }
}
