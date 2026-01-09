/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static java.util.Objects.requireNonNull;

/**
 * Common Factory for creating JDBC query builders with StringTemplate support.
 */
public class JdbcQueryFactory
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcQueryFactory.class);

    private static final String TEST_TEMPLATE = "test_template";
    private final String templateFile;
    private final String localTemplateFile;
    private volatile boolean useLocalFallback = false;

    public JdbcQueryFactory(String templateFile)
    {
        this.templateFile = templateFile;
        this.localTemplateFile = "/tmp/" + templateFile;
    }

    /**
     * Due to a concurrency bug in StringTemplate, we are extracting creation of the template file.
     *
     * @return An STGroupFile instance for the given templateFile.
     */
    private STGroupFile createGroupFile()
    {
        if (!useLocalFallback) {
            try {
                STGroupFile stGroupFile = new STGroupFile(templateFile);
                requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
                return stGroupFile;
            }
            catch (RuntimeException ex) {
                logger.info("createGroupFile: Error while attempting to load STGroupFile for {}.", templateFile, ex);
                return createLocalGroupFile();
            }
        }

        STGroupFile stGroupFile = new STGroupFile(localTemplateFile);
        requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
        return stGroupFile;
    }

    private STGroupFile createLocalGroupFile()
    {
        logger.info("createLocalGroupFile: Attempting STGroupFile fallback for {}.", templateFile);
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(templateFile);
        if (in == null) {
            throw new RuntimeException("Could not find template file: " + templateFile);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = reader.readLine();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(localTemplateFile))) {
                writer.write(sb.toString());
            }
        }
        catch (IOException ex) {
            logger.error("createLocalGroupFile: Exception ", ex);
        }

        useLocalFallback = true;
        STGroupFile stGroupFile = new STGroupFile(localTemplateFile);
        requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
        return stGroupFile;
    }

    public ST getQueryTemplate(String templateName)
    {
        return createGroupFile().getInstanceOf(templateName);
    }
}
