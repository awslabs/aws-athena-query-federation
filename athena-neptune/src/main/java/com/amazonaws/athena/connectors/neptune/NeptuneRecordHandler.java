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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
public class NeptuneRecordHandler extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your
     * catalog id to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "neptune";
    private final NeptuneConnection neptuneConnection;
    private GraphTraversalSource graphTraversalSource;

    // private AmazonS3 amazonS3;

    public NeptuneRecordHandler() {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(), new NeptuneConnection());
    }

    @VisibleForTesting
    protected NeptuneRecordHandler(final AmazonS3 amazonS3, final AWSSecretsManager secretsManager,
            final AmazonAthena amazonAthena, final NeptuneConnection neptuneConnection) {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE);
        // this.amazonS3 = amazonS3;
        this.neptuneConnection = neptuneConnection;
        // this.graphTraversalSource = this.neptuneConnection.getTraversalSource();
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
            final QueryStatusChecker queryStatusChecker) {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());
        /*
         * TODO: This is a simplified version of graph traversal which is a select *
         * equivalent
         */
        // Get the label name using which traversal can be done
        final TableName tableName = recordsRequest.getTableName();
        final String labelName = tableName.getTableName();

        long numRows = 0;
        final AtomicLong numResultRows = new AtomicLong(0);
        final Client client = neptuneConnection.getNeptunClientConnection();
        try {
            final GraphTraversalSource graphTraversalSource = neptuneConnection.getTraversalSource(client);
            // final GraphTraversal<Vertex, Map<Object, Object>> result = graphTraversalSource.V().hasLabel(labelName).valueMap();

            String traversal = "g.V().hasLabel('" + labelName + "')"; //add constraints for final evaluation
        

            logger.info("readWithContraint: Neptune Query Constraints Count: " + recordsRequest.getConstraints().getSummary().size());

            if (recordsRequest.getConstraints().getSummary().size() > 0) {

                logger.info("readWithContraint: Neptune Query Constraints: " + recordsRequest.getConstraints().getSummary().toString());

                HashMap<String, ValueSet>  constraints =  (HashMap<String, ValueSet>)recordsRequest.getConstraints().getSummary();
                traversal += flattenContraintsMap(constraints);
            }

            final String finalTraversal = traversal + ".valueMap()"; //add valuemap construct for final evaluation


            logger.info("readWithContraint: Neptune Query " + finalTraversal);

            //Code to build gremlin query from string
            final ConcurrentBindings b = new ConcurrentBindings();
            b.putIfAbsent("g", graphTraversalSource);
            
            final GremlinExecutor ge = GremlinExecutor.build().evaluationTimeout(15000L).globalBindings(b).create();

            CompletableFuture<Object> evalResult = ge.eval(finalTraversal);
            final GraphTraversal<Vertex, Map<Object, Object>> result = (GraphTraversal<Vertex, Map<Object, Object>>) evalResult.get();

            while (result.hasNext() && queryStatusChecker.isQueryRunning()) {
                numRows++;
                try {
                    spiller.writeRows((final Block block, final int rowNum) -> {
                        final Map<Object, Object> obj = result.next();
                        boolean matched = true;

                        for (final Field nextField : recordsRequest.getSchema().getFields()) {
                            final String fieldName = nextField.getName();
                            if (obj.get(fieldName) != null) {
                                final ArrayList<Object> objs = (ArrayList) obj.get(fieldName);
                                final Object value = TypeUtils.coerce(nextField, objs.get(0)); // TODO: This is assuming
                                                                                               // that there's only
                                                                                               // object in the objs
                                                                                               // ArrayList.
                                // This code has to be modified to cater to situations where there can be
                                // multiple objects in the ArrayList
                                try {
                                    matched &= block.offerValue(fieldName, rowNum, value);
                                    if (!matched) {
                                        return 0;
                                    }
                                } catch (final Exception ex) {
                                    throw new RuntimeException("Error while processing field " + fieldName, ex);
                                }
                            }
                        }
                        numResultRows.getAndIncrement();
                        return 1;
                    });
                } catch (final Exception e) {
                    logger.info("readWithContraint: Exception occured " + e);
                }
            }
            logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
        } catch (final Exception e) {
            logger.info("readWithContraint: Exception occured " + e);
        } finally {
            client.close();
        }
    }


    public String flattenContraintsMap(HashMap hashMap){

        final Set<String> setOfkeys = (Set<String>) (hashMap.keySet());
        String flattenedString = "";

        logger.info("inside flattenConstraintMap: ");

        for (String key : setOfkeys) {
            SortedRangeSet value = (SortedRangeSet) hashMap.get(key);
            List<Range> ranges = value.getOrderedRanges();

            for (Range range : ranges) {

                if (!range.getLow().isNullValue()) {

                    logger.info("inside flattenConstraintMap: "+ range.getType().toString().equalsIgnoreCase(Types.MinorType.INT.getType().toString()));

                    if (range.getType().toString().equalsIgnoreCase(Types.MinorType.INT.getType().toString())) {
                        flattenedString += ".has('" + key + "',gt(" + range.getLow().getValue().toString() + "))";
                    }
                }

                if (!range.getHigh().isNullValue()) {
                    if (range.getType().toString().equalsIgnoreCase(Types.MinorType.INT.getType().toString())) {
                        flattenedString += ".has('" + key + "',lt(" + range.getHigh().getValue().toString() + "))";
                    }
                }
            }
        }

        logger.info("inside flattenConstraintMap: " + flattenedString);

        return flattenedString;
    }
}
