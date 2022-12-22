/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.glue;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CATALOG_NAME_ENV_OVERRIDE;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.FUNCTION_ARN_REGEX;

public class GlueUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GlueUtil.class);

    private GlueUtil(){}

    public static Table getGlueTable(MetadataRequest request, TableName tableName, AWSGlue awsGlue)
    {
        com.amazonaws.services.glue.model.GetTableRequest getTableRequest = new com.amazonaws.services.glue.model.GetTableRequest();
        getTableRequest.setCatalogId(getCatalog(request));
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        return result.getTable();
    }

    private static String getCatalog(MetadataRequest request)
    {
        String override = System.getenv(CATALOG_NAME_ENV_OVERRIDE);
        if (override == null) {
            if (request.getContext() != null) {
                String functionArn = request.getContext().getInvokedFunctionArn();
                String functionOwner = getFunctionOwner(functionArn).orElse(null);
                if (functionOwner != null) {
                    LOGGER.debug("Function Owner: " + functionOwner);
                    return functionOwner;
                }
            }
            return request.getIdentity().getAccount();
        }
        return override;
    }

    private static Optional<String> getFunctionOwner(String functionArn)
    {
        if (functionArn != null) {
            Pattern arnPattern = Pattern.compile(FUNCTION_ARN_REGEX);
            Matcher arnMatcher = arnPattern.matcher(functionArn);
            try {
                if (arnMatcher.matches() && arnMatcher.groupCount() > 0 && arnMatcher.group(1) != null) {
                    return Optional.of(arnMatcher.group(1));
                }
            }
            catch (Exception e) {
                LOGGER.warn("Unable to parse owner from function arn: " + functionArn, e);
            }
        }
        return Optional.empty();
    }
}
