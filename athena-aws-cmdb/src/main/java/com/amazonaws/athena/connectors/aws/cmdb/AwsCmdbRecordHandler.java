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

    public AwsCmdbRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), new TableProviderFactory());
    }

    @VisibleForTesting
    protected AwsCmdbRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, TableProviderFactory tableProviderFactory)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
        tableProviders = tableProviderFactory.getTableProviders();
    }

    /**
     * Delegates to the TableProvider that is registered for the requested table.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
    {
        TableProvider tableProvider = tableProviders.get(readRecordsRequest.getTableName());
        tableProvider.readWithConstraint(constraintEvaluator, blockSpiller, readRecordsRequest);
    }
}
