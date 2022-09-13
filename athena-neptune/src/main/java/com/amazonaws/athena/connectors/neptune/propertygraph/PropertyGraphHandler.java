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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.athena.connectors.neptune.NeptuneRecordHandler;
import com.amazonaws.athena.connectors.neptune.propertygraph.Enums.TableSchemaMetaType;
import com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters.EdgeRowWriter;
import com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters.VertexRowWriter;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class PropertyGraphHandler 
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your
     * catalog id to correlate relevant query errors.
     */

    private final NeptuneConnection neptuneConnection;

    @VisibleForTesting
    public PropertyGraphHandler(NeptuneConnection neptuneConnection) 
    {
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
     * @throws Exception
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because
     *       this will limit the BlockSpiller's ability to control Block size. The
     *       resulting increase in Block size may cause failures and reduced
     *       performance.
     */

    public void executeQuery(ReadRecordsRequest recordsRequest, final QueryStatusChecker queryStatusChecker,
            final BlockSpiller spiller) throws Exception 
    {
        logger.debug("readWithConstraint: enter - " + recordsRequest.getSplit());
        long numRows = 0;
        Client client = neptuneConnection.getNeptuneClientConnection();
        GraphTraversalSource graphTraversalSource = neptuneConnection.getTraversalSource(client);
        GraphTraversal graphTraversal = null;
        String labelName = recordsRequest.getTableName().getTableName();
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        String type = recordsRequest.getSchema().getCustomMetadata().get("componenttype");
        String glabel = recordsRequest.getSchema().getCustomMetadata().get("glabel");
        TableSchemaMetaType tableSchemaMetaType = TableSchemaMetaType.valueOf(type.toUpperCase());

        logger.debug("readWithConstraint: schema type is " + tableSchemaMetaType.toString());
        
        //AWS Glue converts table name to lowercase, table property 'glabel' stores Amazon Neptune Vertex/Edge labels to be used in Gremlin query
        if (glabel != null && !glabel.trim().isEmpty()) {
            labelName = glabel;
        }

        if (tableSchemaMetaType != null) {
            switch (tableSchemaMetaType) {
                case VERTEX:
                    graphTraversal = graphTraversalSource.V().hasLabel(labelName);
                    getQueryPartForContraintsMap(graphTraversal, recordsRequest);
                    graphTraversal = graphTraversal.valueMap().with(WithOptions.tokens);

                    for (final Field nextField : recordsRequest.getSchema().getFields()) {
                        VertexRowWriter.writeRowTemplate(builder, nextField);
                    }

                    break;

                case EDGE:
                    graphTraversal = graphTraversalSource.E().hasLabel(labelName);
                    getQueryPartForContraintsMap(graphTraversal, recordsRequest);
                    graphTraversal = graphTraversal.elementMap();

                    for (final Field nextField : recordsRequest.getSchema().getFields()) {
                        EdgeRowWriter.writeRowTemplate(builder, nextField);
                    }

                    break;
            }
        }

        // log string equivalent of gremlin query
        logger.debug("readWithConstraint: enter - "
                + GroovyTranslator.of("g").translate(graphTraversal.asAdmin().getBytecode()));

        final GraphTraversal graphTraversalFinal1 = graphTraversal;
        final GeneratedRowWriter rowWriter1 = builder.build();

        while (graphTraversalFinal1.hasNext() && queryStatusChecker.isQueryRunning()) {
            numRows++;

            spiller.writeRows((final Block block, final int rowNum) -> {
                final Map obj = (Map) graphTraversalFinal1.next();
                return (rowWriter1.writeRow(block, rowNum, (Object) obj) ? 1 : 0);
            });
        }

        logger.info("readWithConstraint: numRows[{}]", numRows);
    }

    /**
     * Used to generate Gremlin Query part for Constraint Map
     * 
     * @param traversal Gremlin Traversal, traversal is updated based on constraints
     *                  map
     * @param hasMap    Constraint Hash Map
     * 
     * @return A Gremlin Query Part equivalent to Contraint.
     */
    public GraphTraversal getQueryPartForContraintsMap(GraphTraversal traversal,
            final ReadRecordsRequest recordsRequest) 
            {
        final Map<String, ValueSet> hashMap = recordsRequest.getConstraints().getSummary();
        if (hashMap.size() == 0) {
            return traversal;
        }

        logger.info("readWithContraint: Constaints Map " + hashMap.toString());

        final Set<String> setOfkeys = (Set<String>) (hashMap.keySet());
        for (final String key : setOfkeys) {
            if (hashMap.get(key) instanceof SortedRangeSet) {
                final List<Range> ranges = ((SortedRangeSet) hashMap.get(key)).getOrderedRanges();

                for (final Range range : ranges) {
                    if (!range.getLow().isNullValue() && !range.getHigh().isNullValue()) {
                        if (range.getLow().getValue().toString().equals(range.getHigh().getValue().toString())) {
                            traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                                    range.getLow().getValue().toString(), range.getType(), range.getLow().getBound(),
                                    GremlinQueryPreProcessor.Operator.EQUALTO);
                            break;
                        }
                    }

                    if (!range.getLow().isNullValue()) {
                        traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                                range.getLow().getValue().toString(), range.getType(), range.getLow().getBound(),
                                GremlinQueryPreProcessor.Operator.GREATERTHAN);
                    }

                    if (!range.getHigh().isNullValue()) {
                        traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                                range.getHigh().getValue().toString(), range.getType(), range.getHigh().getBound(),
                                GremlinQueryPreProcessor.Operator.LESSTHAN);
                    }
                }
            }

            if (hashMap.get(key) instanceof EquatableValueSet) {
                final EquatableValueSet valueSet = ((EquatableValueSet) hashMap.get(key));

                if (valueSet.isWhiteList()) {
                    traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                            valueSet.getValue(0).toString(), valueSet.getType(), null,
                            GremlinQueryPreProcessor.Operator.EQUALTO);
                } 
                else {
                    traversal = GremlinQueryPreProcessor.generateGremlinQueryPart(traversal, key,
                            valueSet.getValue(0).toString(), valueSet.getType(), null,
                            GremlinQueryPreProcessor.Operator.NOTEQUALTO);
                }
            }
        }

        return traversal;
    }
}
