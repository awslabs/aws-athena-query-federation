/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.getGcsCredentialJsonString;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.printJson;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static com.amazonaws.athena.storage.datasource.StorageDatasourceFactory.createDatasource;

public class GcsRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsRecordHandler.class);
    private static final String SOURCE_TYPE = "gcs";

    private final StorageDatasource datasource;

    public GcsRecordHandler() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient());
    }

    /**
     * Constructor that provides access to S3, secret manager and athena
     * <p>
     * @param amazonS3       An instance of AmazonS3
     * @param secretsManager An instance of AWSSecretsManager
     * @param amazonAthena   An instance of AmazonAthena
     */
    @VisibleForTesting
    protected GcsRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        this.datasource = createDatasource(getGcsCredentialJsonString(this.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR))), System.getenv());
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split - it contains information
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker) throws IOException
    {
        printJson(recordsRequest, "ReadRecordsRequest");
        Split split = recordsRequest.getSplit();
        TableName tableName = recordsRequest.getTableName();
        if (this.datasource == null) {
            throw new RuntimeException("Table " + tableName.getTableName() + " not found in schema "
                    + tableName.getSchemaName());
        }
        LOGGER.debug("RecordHandler=GcsRecordHandler|Method=readWithConstraint|Message=bucketName "
                + split.getProperty(TABLE_PARAM_BUCKET_NAME) + ", file name "
                + split.getProperty(TABLE_PARAM_OBJECT_NAME_LIST));

        this.datasource.loadAllTables(tableName.getSchemaName());
        datasource.readRecords(recordsRequest.getSchema(), recordsRequest.getConstraints(),
                recordsRequest.getTableName(), recordsRequest.getSplit(), spiller, queryStatusChecker);
    }
}
