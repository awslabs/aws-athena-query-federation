/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.Enums.GraphType;
import com.amazonaws.athena.connectors.neptune.propertygraph.NeptuneGremlinConnection;
import com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;
import com.amazonaws.athena.connectors.neptune.rdf.RDFHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of an tutorial that will walk you through how to build a
 * connector for your custom data source. The README for this module
 * (athena-neptune) will guide you through preparing your development
 * environment, modifying this example RecordHandler, building, deploying, and
 * then using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual
 * rows level data from your source. Athena will call readWithConstraint(...) on
 * this class for each 'Split' you generated in NeptuneMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g.
 * athena-cloudwatch, athena-docdb, etc...)
 */
public class NeptuneRecordHandler extends RecordHandler 
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your
     * catalog id to correlate relevant query errors.
     */
    
    private NeptuneConnection neptuneConnection = null;
    private java.util.Map<String, String> configOptions = null;

    private static NeptuneConnection createConnection(java.util.Map<String, String> configOptions)
    {
        GraphType graphType = GraphType.PROPERTYGRAPH;
        if (configOptions.get(Constants.CFG_GRAPH_TYPE) != null) {
            graphType = GraphType.valueOf(configOptions.get(Constants.CFG_GRAPH_TYPE).toUpperCase());
        }

        switch(graphType){
            case PROPERTYGRAPH: 
                return new NeptuneGremlinConnection(configOptions.get(Constants.CFG_ENDPOINT),
                configOptions.get(Constants.CFG_PORT), Boolean.parseBoolean(configOptions.get(Constants.CFG_IAM)), 
                configOptions.get(Constants.CFG_REGION));

            case RDF:
                return new NeptuneSparqlConnection(configOptions.get(Constants.CFG_ENDPOINT),
                        configOptions.get(Constants.CFG_PORT), Boolean.parseBoolean(configOptions.get(Constants.CFG_IAM)), 
                        configOptions.get(Constants.CFG_REGION));
        }
        return null;
    }

    public NeptuneRecordHandler(java.util.Map<String, String> configOptions) 
    {
        this(
            AmazonS3ClientBuilder.defaultClient(),
            AWSSecretsManagerClientBuilder.defaultClient(),
            AmazonAthenaClientBuilder.defaultClient(),
            createConnection(configOptions),
            configOptions);
    }

    @VisibleForTesting
    protected NeptuneRecordHandler(
        AmazonS3 amazonS3,
        AWSSecretsManager secretsManager,
        AmazonAthena amazonAthena,
        NeptuneConnection neptuneConnection,
        java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, amazonAthena, Constants.SOURCE_TYPE, configOptions);
        this.neptuneConnection = neptuneConnection;
        this.configOptions = configOptions;
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row
     *                           data associated with this Split. The BlockSpiller
     *                           automatically handles chunking the response,
     *                           encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including: 1. The
     *                           Split 2. The Catalog, Database, and Table the read
     *                           request is for. 3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing
     *                           work for a query that has already terminated
     * @throws Exception
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because
     *       this will limit the BlockSpiller's ability to control Block size. The
     *       resulting increase in Block size may cause failures and reduced
     *       performance.
     */
    @Override
    protected void readWithConstraint(final BlockSpiller spiller, final ReadRecordsRequest recordsRequest, 
     final QueryStatusChecker queryStatusChecker) throws Exception 
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());
        GraphType graphType = GraphType.PROPERTYGRAPH;

        if (configOptions.get(Constants.CFG_GRAPH_TYPE) != null) {
            graphType = GraphType.valueOf(configOptions.get(Constants.CFG_GRAPH_TYPE).toUpperCase());
        }

        try {
            switch(graphType){
                case PROPERTYGRAPH:
                    (new PropertyGraphHandler(neptuneConnection)).executeQuery(recordsRequest, queryStatusChecker, spiller, configOptions);
                    break;

                case RDF:
                    (new RDFHandler(neptuneConnection)).executeQuery(recordsRequest, queryStatusChecker, spiller, configOptions);   
                    break;
            }
        } 
        catch (final ClassCastException e) {
            logger.info("readWithContraint: Exception occured " + e);
            throw new RuntimeException(
                    "Error occurred while fetching records, please refer to cloudwatch logs for more details");
        } 
        catch (final IllegalArgumentException e) {
            logger.info("readWithContraint: Exception occured " + e);
            throw new RuntimeException(
                    "Error occurred while fetching records, please refer to cloudwatch logs for more details");
        } 
        catch (final RuntimeException e) {
            logger.info("readWithContraint: Exception occured " + e);
            throw e;
        } 
    }
}
