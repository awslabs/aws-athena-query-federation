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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    //Controls the page size for fetching batches of documents from the MongoDB client.
    private static final int MONGO_QUERY_BATCH_SIZE = 100;

    private final DocDBConnectionFactory connectionFactory;

    public DocDBRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), new DocDBConnectionFactory());
    }

    @VisibleForTesting
    protected DocDBRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, DocDBConnectionFactory connectionFactory)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
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
        String conStr = split.getProperty(DOCDB_CONN_STR);
        if (conStr == null) {
            throw new RuntimeException(DOCDB_CONN_STR + " Split property is null! Unable to create connection.");
        }
        String endpoint = resolveSecrets(conStr);
        return connectionFactory.getOrCreateConn(endpoint);
    }

    /**
     * Scans DocumentDB using the scan settings set on the requested Split by DocDBeMetadataHandler.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        TableName tableName = recordsRequest.getTableName();
        Map<String, ValueSet> constraintSummary = recordsRequest.getConstraints().getSummary();

        MongoClient client = getOrCreateConn(recordsRequest.getSplit());
        MongoDatabase db = client.getDatabase(tableName.getSchemaName());
        MongoCollection<Document> table = db.getCollection(tableName.getTableName());

        Document query = QueryUtils.makeQuery(recordsRequest.getSchema(), constraintSummary);
        Document output = QueryUtils.makeProjection(recordsRequest.getSchema());

        final MongoCursor<Document> iterable = table
                .find(query)
                .projection(output)
                .batchSize(MONGO_QUERY_BATCH_SIZE).iterator();

        long numRows = 0;
        AtomicLong numResultRows = new AtomicLong(0);
        while (iterable.hasNext()) {
            numRows++;
            spiller.writeRows((Block block, int rowNum) -> {
                Document doc = iterable.next();

                boolean matched = true;
                for (Field nextField : recordsRequest.getSchema().getFields()) {
                    if (!matched) {
                        break;
                    }
                    matched &= constraintEvaluator.apply(nextField.getName(),
                            TypeUtils.coerce(nextField, doc.get(nextField.getName())));
                }

                if (matched) {
                    numResultRows.getAndIncrement();
                    for (Field nextField : recordsRequest.getSchema().getFields()) {
                        FieldVector vector = block.getFieldVector(nextField.getName());
                        Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                        Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                        try {
                            switch (fieldType) {
                                case LIST:
                                    BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, value);
                                    break;
                                case STRUCT:
                                    BlockUtils.setComplexValue(vector, rowNum,
                                            (Field field, Object val) -> ((Document) val).get(field.getName()), value);
                                    break;
                                default:
                                    BlockUtils.setValue(vector, rowNum, value);
                                    break;
                            }
                        }
                        catch (Exception ex) {
                            throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                        }
                    }
                }

                return matched ? 1 : 0;
            });
        }

        logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
    }
}
