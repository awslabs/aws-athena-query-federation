/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.util.LimitAndSortHelper;
import com.amazonaws.athena.connectors.docdb.qpt.DocDBQueryPassthrough;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.substrait.proto.Plan;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connector.substrait.SubstraitRelUtils.deserializeSubstraitPlan;
import static com.amazonaws.athena.connectors.docdb.DocDBFieldResolver.DEFAULT_FIELD_RESOLVER;
import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;

/**
 * Handles data read record requests for the Athena DocumentDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Attempts to resolve sensitive configuration fields such as HBase connection string via SecretsManager so that you can
 * substitute variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class DocDBRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandler.class);

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "documentdb";
    //The env secret_name to use if defined
    private static final String SECRET_NAME = "secret_name";
    //Controls the page size for fetching batches of documents from the MongoDB client.
    private static final int MONGO_QUERY_BATCH_SIZE = 100;

    // This needs to be turned on if the user is using a Glue table and their docdb tables contain cased column names
    private static final String DISABLE_PROJECTION_AND_CASING_ENV = "disable_projection_and_casing";

    private final DocDBConnectionFactory connectionFactory;

    private final DocDBQueryPassthrough queryPassthrough = new DocDBQueryPassthrough();

    public DocDBRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
                S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(),
                new DocDBConnectionFactory(),
                configOptions);
    }

    @VisibleForTesting
    protected DocDBRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, DocDBConnectionFactory connectionFactory, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.connectionFactory = connectionFactory;
    }

    /**
     * Gets the special DOCDB_CONN_STR property from the provided split and uses its contents to getOrCreate
     * a MongoDB client connection.
     *
     * @param split The split to that we need to read and this the DocDB instance to connecto ro.
     * @return A MongoClient connected to the request DB instance.
     * @note This method attempts to resolve any SecretsManager secrets that are using in the connection string and denoted
     * by ${secret_name}.
     */
    private MongoClient getOrCreateConn(Split split)
    {
        String connStr = split.getProperty(DOCDB_CONN_STR);
        if (connStr == null) {
            throw new RuntimeException(DOCDB_CONN_STR + " Split property is null! Unable to create connection.");
        }
        String endpoint = resolveWithDefaultCredentials(connStr);
        return connectionFactory.getOrCreateConn(endpoint);
    }

    private static Map<String, Object> documentAsMap(Document document, boolean caseInsensitive)
    {
        logger.info("documentAsMap: caseInsensitive: {}", caseInsensitive);
        Map<String, Object> documentAsMap = (Map<String, Object>) document;
        if (!caseInsensitive) {
            return documentAsMap;
        }

        TreeMap<String, Object> caseInsensitiveMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(documentAsMap);
        return caseInsensitiveMap;
    }

    /**
     * Scans DocumentDB using the scan settings set on the requested Split by DocDBMetadataHandler.
     * This method handles query execution with various optimizations including predicate pushdown,
     * limit pushdown, sort pushdown, and projection optimization.
     *
     * @param spiller The BlockSpiller to write results to
     * @param recordsRequest The ReadRecordsRequest containing query details and constraints
     * @param queryStatusChecker Used to check if the query is still running
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        final TableName tableNameObj = recordsRequest.getTableName();
        final String schemaName = tableNameObj.getSchemaName();
        final String tableName = recordsRequest.getSchema().getCustomMetadata().getOrDefault(
                SOURCE_TABLE_PROPERTY, tableNameObj.getTableName());

        logger.info("Starting readWithConstraint for schema: {}, table: {}", schemaName, tableName);

        final Map<String, ValueSet> constraintSummary = recordsRequest.getConstraints().getSummary();
        logger.info("Processing {} constraints", constraintSummary.size());

        final MongoClient client = getOrCreateConn(recordsRequest.getSplit());
        final MongoDatabase db;
        final MongoCollection<Document> table;
        Document query;

        // ---------------------- Substrait Plan extraction ----------------------
        final QueryPlan queryPlan = recordsRequest.getConstraints().getQueryPlan();
        final Plan plan;
        final boolean hasQueryPlan;
        if (queryPlan != null) {
            hasQueryPlan = true;
            plan = deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
            logger.info("Using Substrait query plan for optimization");
        }
        else {
            hasQueryPlan = false;
            plan = null;
            logger.info("No Substrait query plan available, using constraint-based filtering");
        }

        // ---------------------- LIMIT pushdown support ----------------------
        final Pair<Boolean, Integer> limitPair = getLimit(plan, recordsRequest.getConstraints());
        final boolean hasLimit = limitPair.getLeft();
        final int limit = limitPair.getRight();
        if (hasLimit) {
            logger.info("LIMIT pushdown enabled with limit: {}", limit);
        }

        // ---------------------- SORT pushdown support ----------------------
        final Pair<Boolean, List<LimitAndSortHelper.GenericSortField>> sortPair = getSortFromPlan(plan);
        final boolean hasSort = sortPair.getLeft();
        final Document sortDoc = convertToMongoSort(sortPair.getRight());

        // ---------------------- Query construction ----------------------
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            final Map<String, String> qptArguments = recordsRequest.getConstraints().getQueryPassthroughArguments();
            queryPassthrough.verify(qptArguments);
            db = client.getDatabase(qptArguments.get(DocDBQueryPassthrough.DATABASE));
            table = db.getCollection(qptArguments.get(DocDBQueryPassthrough.COLLECTION));
            query = QueryUtils.parseFilter(qptArguments.get(DocDBQueryPassthrough.FILTER));
        }
        else {
            db = client.getDatabase(schemaName);
            table = db.getCollection(tableName);
            final Map<String, List<ColumnPredicate>> columnPredicateMap = QueryUtils.buildFilterPredicatesFromPlan(plan);
            if (!columnPredicateMap.isEmpty()) {
                // Use enhanced query generation that preserves AND/OR logical structure from SQL via the Query Plan
                query = QueryUtils.makeEnhancedQueryFromPlan(plan);
            }
            else {
                query = QueryUtils.makeQuery(recordsRequest.getSchema(), recordsRequest.getConstraints().getSummary());
            }
        }

        final String disableProjectionAndCasingEnvValue = configOptions.getOrDefault(DISABLE_PROJECTION_AND_CASING_ENV, "false").toLowerCase();
        final boolean disableProjectionAndCasing = disableProjectionAndCasingEnvValue.equals("true");
        logger.info("Projection and casing configuration - environment value: {}, resolved: {}",
                disableProjectionAndCasingEnvValue, disableProjectionAndCasing);

        // TODO: Currently AWS DocumentDB does not support collation, which is required for case insensitive indexes:
        // https://www.mongodb.com/docs/manual/core/index-case-insensitive/
        // Once AWS DocumentDB supports collation, then projections do not have to be disabled anymore because case
        // insensitive indexes allows for case insensitive projections.
        final Document projection = disableProjectionAndCasing ? null : QueryUtils.makeProjection(recordsRequest.getSchema());
        logger.info("readWithConstraint: query[{}] projection[{}]", query, projection);

        // ---------------------- Build and execute query ----------------------
        FindIterable<Document> findIterable = table.find(query).projection(projection);

        // Apply SORT pushdown first (should be before LIMIT for correct semantics)
        if (hasSort && !sortDoc.isEmpty()) {
            findIterable = findIterable.sort(sortDoc);
            logger.info("Applied ORDER BY pushdown");
        }

        // Apply LIMIT pushdown after SORT
        if (hasLimit) {
            findIterable = findIterable.limit(limit);
            logger.info("Applied LIMIT pushdown: {}", limit);
        }

        final MongoCursor<Document> iterable = findIterable.batchSize(MONGO_QUERY_BATCH_SIZE).iterator();

        long numRows = 0;
        final AtomicLong numResultRows = new AtomicLong(0);
        while (iterable.hasNext() && queryStatusChecker.isQueryRunning()) {
            if (hasLimit && numRows >= limit) {
                logger.info("Reached configured limit of {} rows, stopping iteration", limit);
                break;
            }
            numRows++;

            spiller.writeRows((Block block, int rowNum) -> {
                final Map<String, Object> doc = documentAsMap(iterable.next(), disableProjectionAndCasing);
                boolean matched = true;

                for (final Field nextField : recordsRequest.getSchema().getFields()) {
                    final Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                    final Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());

                    try {
                        switch (fieldType) {
                            case LIST:
                            case STRUCT:
                                matched &= block.offerComplexValue(nextField.getName(), rowNum, DEFAULT_FIELD_RESOLVER, value);
                                break;
                            default:
                                matched &= block.offerValue(nextField.getName(), rowNum, value, hasQueryPlan);
                                break;
                        }
                        if (!matched) {
                            return 0;
                        }
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                    }
                }

                numResultRows.getAndIncrement();
                return 1;
            });
        }

        logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
    }

    /**
     * Converts generic sort fields to MongoDB sort document format.
     *
     * @param sortFields List of generic sort fields
     * @return MongoDB Document with sort specifications (1 for ASC, -1 for DESC)
     */
    private Document convertToMongoSort(List<LimitAndSortHelper.GenericSortField> sortFields)
    {
        Document sortDoc = new Document();
        if (sortFields != null) {
            for (LimitAndSortHelper.GenericSortField field : sortFields) {
                int direction = field.isAscending() ? 1 : -1;
                sortDoc.put(field.getColumnName().toLowerCase(), direction);
            }
        }
        return sortDoc;
    }
}
