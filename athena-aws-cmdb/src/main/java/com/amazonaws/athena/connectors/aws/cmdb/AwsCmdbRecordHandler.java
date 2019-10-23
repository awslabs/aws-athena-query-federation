package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;

import java.util.Map;

public class AwsCmdbRecordHandler
        extends RecordHandler
{
    private static final String sourceType = "cmdb";
    private Map<TableName, TableProvider> tableProviders;

    public AwsCmdbRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), new TableProviderFactory());
    }

    @VisibleForTesting
    protected AwsCmdbRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, TableProviderFactory tableProviderFactory)
    {
        super(amazonS3, secretsManager, sourceType);
        tableProviders = tableProviderFactory.getTableProviders();
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
    {
        TableProvider tableProvider = tableProviders.get(readRecordsRequest.getTableName());
        tableProvider.readWithConstraint(constraintEvaluator, blockSpiller, readRecordsRequest);
    }
}
