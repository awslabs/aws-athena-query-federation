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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
    private static final String SOURCE_TYPE = "neptune";
    private final NeptuneConnection neptuneConnection;

    public NeptuneRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                new NeptuneConnection(System.getenv("neptune_endpoint"), System.getenv("neptune_port")));
    }

    @VisibleForTesting
    protected NeptuneRecordHandler(final AmazonS3 amazonS3, final AWSSecretsManager secretsManager,
            final AmazonAthena amazonAthena, final NeptuneConnection neptuneConnection)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        this.neptuneConnection = neptuneConnection;
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
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because
     *       this will limit the BlockSpiller's ability to control Block size. The
     *       resulting increase in Block size may cause failures and reduced
     *       performance.
     */
    @Override
    protected void readWithConstraint(final BlockSpiller spiller, final ReadRecordsRequest recordsRequest,
            final QueryStatusChecker queryStatusChecker)
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());
        TableName tableName = recordsRequest.getTableName();
        String labelName = tableName.getTableName();
        long numRows = 0;
        AtomicLong numResultRows = new AtomicLong(0);
        Client client = null;
        GraphTraversalSource graphTraversalSource = null;

        try {
            client = neptuneConnection.getNeptuneClientConnection();
            graphTraversalSource = neptuneConnection.getTraversalSource(client);

            GraphTraversal<Vertex, Vertex> graphTraversal = graphTraversalSource.V().hasLabel(labelName);

            if (recordsRequest.getConstraints().getSummary().size() > 0) {
                logger.info(
                        "readWithContraint: Constaints Map " + recordsRequest.getConstraints().getSummary().toString());

                final Map<String, ValueSet> constraints = recordsRequest.getConstraints().getSummary();
                graphTraversal = getQueryPartForContraintsMap(graphTraversal, constraints);
            }

            GraphTraversal<Vertex, Map<Object, Object>> graphTraversalFinal = graphTraversal.valueMap();

            // log string equivalent of gremlin query
            logger.info("readWithConstraint: enter - "
                    + GroovyTranslator.of("g").translate(graphTraversalFinal.asAdmin().getBytecode()));

            GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

            for (final Field nextField : recordsRequest.getSchema().getFields()) {
                builder = TypeRowWriter.writeRowTemplate(builder, nextField);
            }

            GeneratedRowWriter rowWriter = builder.build();

            while (graphTraversalFinal.hasNext() && queryStatusChecker.isQueryRunning()) {
                numRows++;
                try {
                    spiller.writeRows((final Block block, final int rowNum) -> {
                        final Map<Object, Object> obj = graphTraversalFinal.next();
                        return (rowWriter.writeRow(block, rowNum, (Object) obj) ? 1 : 0);
                    });
                } 
                catch (final Exception e) {
                    logger.info("readWithContraint: Exception occured " + e);
                }
            }
            logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
        } 
        catch (final Exception e) {
            logger.info("readWithContraint: Exception occured " + e);
        } 
        finally {
            if (client != null) {
                client.close();
            }           
        }
    }

    /**
     * Used to generate Gremlin Query part for Constraint Map
     * 
     * @param traversal Gremlin Traversal, traversal is updated based on constraints map
     * @param hasMap Constraint Hash Map
     * 
     * @return A Gremlin Query Part equivalent to Contraint.
     */
    public GraphTraversal<Vertex, Vertex> getQueryPartForContraintsMap(GraphTraversal<Vertex, Vertex> traversal,
            final Map hashMap)
    {
        final Set<String> setOfkeys = (Set<String>) (hashMap.keySet());
        for (final String key : setOfkeys) {
            final List<Range> ranges = ((SortedRangeSet) hashMap.get(key)).getOrderedRanges();

            for (final Range range : ranges) {
                if (!range.getLow().isNullValue() && !range.getHigh().isNullValue()) {
                    if (range.getLow().getValue().toString().equals(range.getHigh().getValue().toString())) {
                        traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                                range.getLow().getValue().toString(), range.getType(),
                                range.getLow().getBound(), GremlinQueryPreProcessor.Operator.EQUALTO);
                        break;
                    }
                }

                if (!range.getLow().isNullValue()) {
                    logger.info("inside flattenConstraintMap: "
                            + range.getType().toString().equalsIgnoreCase(Types.MinorType.INT.getType().toString()));

                    traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                            range.getLow().getValue().toString(), range.getType(), range.getLow().getBound(),
                            GremlinQueryPreProcessor.Operator.GREATERTHAN);
                }

                if (!range.getHigh().isNullValue()) {
                    traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                            range.getHigh().getValue().toString(), range.getType(),
                            range.getLow().getBound(), GremlinQueryPreProcessor.Operator.LESSTHAN);
                }
            }
        }

        return traversal;
    }
}
