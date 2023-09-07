/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;

import java.util.Map;

/**
 * Handles record requests for the Athena AWS CMDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Maps AWS Resources to SQL tables using a set of TableProviders constructed from a TableProviderFactory.
 * 2. This class is largely a mux that delegates requests to the appropriate TableProvider based on the
 * requested TableName.
 */
public class AwsCmdbRecordHandler
        extends RecordHandler
{
    private static final String SOURCE_TYPE = "cmdb";

    //Map of available fully qualified TableNames to their respective TableProviders.
    private Map<TableName, TableProvider> tableProviders;

    public AwsCmdbRecordHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        tableProviders = new TableProviderFactory(configOptions).getTableProviders();
    }

    @VisibleForTesting
    protected AwsCmdbRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, TableProviderFactory tableProviderFactory, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        tableProviders = tableProviderFactory.getTableProviders();
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
    {
        TableProvider tableProvider = tableProviders.get(readRecordsRequest.getTableName());
        tableProvider.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }
}
