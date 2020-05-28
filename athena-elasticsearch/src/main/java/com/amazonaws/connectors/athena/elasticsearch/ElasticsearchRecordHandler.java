/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.elasticsearch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-elasticsearch) will guide you through preparing
 * your development environment, modifying this elasticsearch RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ElasticsearchMetadataHandler.
 * <p>
 * For more elasticsearchs, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ElasticsearchRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "elasticsearch";

    // Pagination batch size (100 documents).
    private static final int QUERY_BATCH_SIZE = 100;

    private AmazonS3 amazonS3;

    private final AwsRestHighLevelClientFactory clientFactory;
    private final ElasticsearchQueryUtils queryUtils;
    private final ElasticsearchTypeUtils typeUtils;
    private final ElasticsearchHelper helper;

    public ElasticsearchRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(), ElasticsearchHelper.getInstance().getClientFactory());
    }

    @VisibleForTesting
    protected ElasticsearchRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena,
                                         AwsRestHighLevelClientFactory clientFactory)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.clientFactory = clientFactory;
        this.queryUtils = new ElasticsearchQueryUtils();
        this.typeUtils = new ElasticsearchTypeUtils();
        this.helper = ElasticsearchHelper.getInstance();
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                                      QueryStatusChecker queryStatusChecker)
    {
        logger.info("readWithConstraint - enter\n\nDomain: {}\n\nIndex: {}\n\nMapping: {}",
                recordsRequest.getTableName().getSchemaName(), recordsRequest.getTableName().getTableName(),
                recordsRequest.getSchema());

        String endpoint = helper.getDomainEndpoint(recordsRequest.getTableName().getSchemaName());
        long numRows = 0;

        if (!endpoint.isEmpty() && queryStatusChecker.isQueryRunning()) {
            AwsRestHighLevelClient client = clientFactory.getClient(endpoint);
            try {
                // Create field extractors for all data types in the schema.
                GeneratedRowWriter.RowWriterBuilder builder =
                        GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
                for (Field field : recordsRequest.getSchema().getFields()) {
                    Extractor extractor = typeUtils.makeExtractor(field);
                    if (extractor != null) {
                        builder.withExtractor(field.getName(), extractor);
                    }
                    else {
                        builder.withFieldWriterFactory(field.getName(), typeUtils.makeFactory(field));
                    }
                }
                // Create a new search-source injected with the projection, predicate, and the pagination batch size.
                SearchSourceBuilder searchSource = new SearchSourceBuilder().size(QUERY_BATCH_SIZE)
                        .fetchSource(queryUtils.getProjection(recordsRequest.getSchema()))
                        .query(queryUtils.getQuery(recordsRequest.getConstraints().getSummary()));
                // Create a new search-request for the specified index.
                SearchRequest searchRequest = new SearchRequest(recordsRequest.getTableName().getTableName());
                int hitsNum;
                int currPosition = 0;
                do {
                    // Process the search request injecting the search-source, and setting the from position
                    // used for pagination of results.
                    SearchResponse searchResponse = client
                            .getDocuments(searchRequest.source(searchSource.from(currPosition)));
                    // Increment current position to next batch of results.
                    currPosition += QUERY_BATCH_SIZE;
                    // Process hits.
                    Iterator<SearchHit> hitIterator = searchResponse.getHits().iterator();
                    hitsNum = searchResponse.getHits().getHits().length;
                    GeneratedRowWriter rowWriter = builder.build();

                    while (hitIterator.hasNext() && queryStatusChecker.isQueryRunning()) {
                        ++numRows;
                        spiller.writeRows((Block block, int rowNum) ->
                                rowWriter.writeRow(block, rowNum, client.getDocument(hitIterator.next())) ? 1 : 0);
                    }
                    // if hitsNum < QUERY_BATCH_SIZE, then this is the last batch of documents.
                } while (hitsNum == QUERY_BATCH_SIZE && queryStatusChecker.isQueryRunning());
            }
            catch (IOException error) {
                logger.error("Error sending query:", error);
            }
            finally {
                client.shutdown();
            }
        }

        logger.info("readWithConstraint: numRows[{}]", numRows);
    }

    @VisibleForTesting
    protected int getQueryBatchSize()
    {
        return QUERY_BATCH_SIZE;
    }
}
