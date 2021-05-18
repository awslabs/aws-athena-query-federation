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
package com.amazonaws.athena.connector.integ.providers;

import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Responsible for providing the Connector's packaging attributes used in creating the Connector's stack attributes.
 */
public class ConnectorPackagingAttributesProvider
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorPackagingAttributesProvider.class);

    private static final String CF_TEMPLATE_NAME = "packaged.yaml";
    private static final String LAMBDA_CODE_URI_TAG = "CodeUri:";
    private static final String LAMBDA_SPILL_BUCKET_PREFIX = "s3://";
    private static final String LAMBDA_HANDLER_TAG = "Handler:";
    private static final String LAMBDA_HANDLER_PREFIX = "Handler: ";

    private ConnectorPackagingAttributesProvider() {}
    
    /**
     * Extracts the packaging attributes needed in the creation of the CF Stack from packaged.yaml (S3 Bucket,
     * S3 Key, and lambdaFunctionHandler).
     * @return Connector's packaging attributes (S3 Bucket, S3 Key, and Lambda function handler).
     * @throws RuntimeException CloudFormation template (packaged.yaml) was not found.
     */
    public static ConnectorPackagingAttributes getAttributes()
            throws RuntimeException
    {
        String s3Bucket = "";
        String s3Key = "";
        String lambdaFunctionHandler = "";

        try {
            for (String line : Files.readAllLines(Paths.get(CF_TEMPLATE_NAME), StandardCharsets.UTF_8)) {
                if (line.contains(LAMBDA_CODE_URI_TAG)) {
                    s3Bucket = line.substring(line.indexOf(LAMBDA_SPILL_BUCKET_PREFIX) +
                            LAMBDA_SPILL_BUCKET_PREFIX.length(), line.lastIndexOf('/'));
                    s3Key = line.substring(line.lastIndexOf('/') + 1);
                }
                else if (line.contains(LAMBDA_HANDLER_TAG)) {
                    lambdaFunctionHandler = line.substring(line.indexOf(LAMBDA_HANDLER_PREFIX) +
                            LAMBDA_HANDLER_PREFIX.length());
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Lambda connector has not been packaged via `sam package` (see README).", e);
        }

        logger.info("S3 Bucket: [{}], S3 Key: [{}], Handler: [{}]", s3Bucket, s3Key, lambdaFunctionHandler);

        return new ConnectorPackagingAttributes(s3Bucket, s3Key, lambdaFunctionHandler);
    }
}
