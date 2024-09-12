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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneQueryPassthrough;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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

public class RDFHandler 
{
    private static final Logger logger = LoggerFactory.getLogger(RDFHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your
     * catalog id to correlate relevant query errors.
     */

    private final NeptuneSparqlConnection neptuneConnection;
    private final NeptuneQueryPassthrough queryPassthrough = new NeptuneQueryPassthrough();

    // @VisibleForTesting
    public RDFHandler(NeptuneConnection neptuneConnection) throws Exception
    {
        this.neptuneConnection = (NeptuneSparqlConnection) neptuneConnection;
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
    

    /**
     * Performance considerations:
     * If no constraints are provided, it gets THE WHOLE DATASET. That's so slow the Lamdbda this is running it might time out.
     * Remedy:
     * - Client always uses constraints to restrict output
     * - For tables based on class, add a LIMIT that is configurable as lambda environment var
     * - For tables based on sparql, allow LIMIT to be used
     */
    public void executeQuery(ReadRecordsRequest recordsRequest, final QueryStatusChecker queryStatusChecker,
            final BlockSpiller spiller, java.util.Map<String, String> configOptions)  throws Exception 
    {
        StringBuilder sparql;
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            Map<String, String> qptArguments = recordsRequest.getConstraints().getQueryPassthroughArguments();
            queryPassthrough.verify(qptArguments);
            sparql = new StringBuilder(qptArguments.get(NeptuneQueryPassthrough.QUERY));
        }
        else {
            // 1. Get the specified prefixes
            Map<String, String> prefixMap = new HashMap<>();
            StringBuilder prefixBlock = new StringBuilder("");
            for (String k : recordsRequest.getSchema().getCustomMetadata().keySet()) {
                if (k.startsWith(Constants.PREFIX_KEY)) {
                    String pfx = k.substring(Constants.PREFIX_LEN);
                    String val = recordsRequest.getSchema().getCustomMetadata().get(k);
                    prefixMap.put(pfx, val);
                    prefixBlock.append("PREFIX " + pfx + ": <" + val + ">\n");
                }
            }

            // 2. Build the SPARQL query
            String queryMode = recordsRequest.getSchema().getCustomMetadata().get(Constants.SCHEMA_QUERY_MODE);
            if (queryMode == null) {
                throw new RuntimeException("Mandatory: querymode");
            }
            queryMode = queryMode.toLowerCase();
            sparql = new StringBuilder(prefixBlock + "\n");
            if (queryMode.equals(Constants.QUERY_MODE_SPARQL)) {
                String sparqlParam = recordsRequest.getSchema().getCustomMetadata().get(Constants.QUERY_MODE_SPARQL);
                if (sparqlParam == null) {
                    throw new RuntimeException("Mandatory: sparql when querympde=sparql");
                }
                sparql.append("\n" + sparqlParam);
            }
            else if (queryMode.equals(Constants.QUERY_MODE_CLASS)) {
                String classURI = recordsRequest.getSchema().getCustomMetadata().get(Constants.SCHEMA_CLASS_URI);
                String predsPrefix = recordsRequest.getSchema().getCustomMetadata().get(Constants.SCHEMA_PREDS_PREFIX);
                String subject = recordsRequest.getSchema().getCustomMetadata().get(Constants.SCHEMA_SUBJECT);
                if (classURI == null) {
                    throw new RuntimeException("Mandatory: classuri when querymode=class");
                }
                if (predsPrefix == null) {
                    throw new RuntimeException("Mandatory: predsPrefix when querymode=class");
                }
                if (subject == null) {
                    throw new RuntimeException("Mandatory:subject when querymode=class");
                }
                sparql.append("\nselect ");
                for (Field prop : recordsRequest.getSchema().getFields()) {
                    sparql.append("?" + prop.getName() + " ");
                }
                sparql.append(" WHERE {");
                sparql.append("\n?" + subject + " a " + classURI + " . ");
                for (Field prop : recordsRequest.getSchema().getFields()) {
                    if (!prop.getName().equals(subject)) {
                        sparql.append("\n?" + subject + " " + predsPrefix + ":" + prop.getName() + " ?" + prop.getName()
                                + " .");
                    }
                }
                sparql.append(" }");
            }
            else {
                throw new RuntimeException("Illegal RDF params");
            }
        }

        // 3. Create the builder and add row writer exttractors for each field
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter
                .newBuilder(recordsRequest.getConstraints());
        for (final Field nextField : recordsRequest.getSchema().getFields()) {
            SparqlRowWriter.writeRowTemplate(builder, nextField);
        }

        final GeneratedRowWriter rowWriter = builder.build();

        // get results
        String strim = recordsRequest.getSchema().getCustomMetadata().get(Constants.SCHEMA_STRIP_URI);
        boolean trimURI = strim == null ? false : Boolean.parseBoolean(strim);
        neptuneConnection.runQuery(sparql.toString());
        while (neptuneConnection.hasNext() && queryStatusChecker.isQueryRunning()) {
            Map<String, Object> result = neptuneConnection.next(trimURI);
            spiller.writeRows((final Block block, final int rowNum) -> {
                return (rowWriter.writeRow(block, rowNum, (Object) result) ? 1 : 0);
            });
        }
    }
}
