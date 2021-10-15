/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.query;

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

import static org.apache.commons.lang3.Validate.notNull;

public class QueryFactory
{
    private static final Logger logger = LoggerFactory.getLogger(QueryFactory.class);

    private static final String TEMPLATE_FILE = "Timestream.stg";
    private static final String LOCAL_TEMPLATE_FILE = "/tmp/Timestream.stg";
    private static final String TEST_TEMPLATE = "test_template";
    private volatile boolean useLocalFallback = false;

    /**
     * Due to a concurrency bug in StringTemplate, we are extracting creation of the template file.
     *
     * @return An STGroupFile instance for the given templateFile.
     */
    private STGroupFile createGroupFile()
    {
        if (!useLocalFallback) {
            try {
                STGroupFile stGroupFile = new STGroupFile(TEMPLATE_FILE);
                notNull(stGroupFile.getInstanceOf(TEST_TEMPLATE));
                return stGroupFile;
            }
            catch (RuntimeException ex) {
                logger.info("createGroupFile: Error while attempting to load STGroupFile.", ex);
                return createLocalGroupFile();
            }
        }

        STGroupFile stGroupFile = new STGroupFile(LOCAL_TEMPLATE_FILE);
        notNull(stGroupFile.getInstanceOf(TEST_TEMPLATE));
        return stGroupFile;
    }

    private STGroupFile createLocalGroupFile()
    {
        logger.info("createLocalGroupFile: Attempting STGroupFile fallback.");
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(TEMPLATE_FILE);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            sb.append(line);
            while (line != null) {
                line = reader.readLine();
                if (line != null) {
                    sb.append(line);
                }
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(LOCAL_TEMPLATE_FILE));
            writer.write(sb.toString());
            writer.close();
        }
        catch (IOException ex) {
            logger.error("createLocalGroupFile: Exception ", ex);
        }

        useLocalFallback = true;
        logger.info("createLocalGroupFile: {}", sb.toString());

        STGroupFile stGroupFile = new STGroupFile(LOCAL_TEMPLATE_FILE);
        notNull(stGroupFile.getInstanceOf(TEST_TEMPLATE));
        return stGroupFile;
    }

    /**
     * Used to get an instance of a templated DESCRIBE query.
     *
     * @param templateName The name of the query template.
     * @return The StringTemplate containing the query template that can be used to render and instance of the query template.
     */
    private ST getQueryTemplate(String templateName)
    {
        return createGroupFile().getInstanceOf(templateName);
    }

    public DescribeTableQueryBuilder createDescribeTableQueryBuilder()
    {
        return new DescribeTableQueryBuilder(getQueryTemplate(DescribeTableQueryBuilder.getTemplateName()));
    }

    public SelectQueryBuilder createSelectQueryBuilder(String viewPropertyName)
    {
        return new SelectQueryBuilder(getQueryTemplate(SelectQueryBuilder.getTemplateName()), viewPropertyName);
    }
}
