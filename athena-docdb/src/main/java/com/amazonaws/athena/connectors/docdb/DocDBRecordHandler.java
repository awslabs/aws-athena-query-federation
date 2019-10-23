package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
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
import com.mongodb.client.MongoClients;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DocDBRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandler.class);

    private static final String SOURCE_TYPE = "documentdb";
    private static final int MONGO_QUERY_BATCH_SIZE = 100;

    private final Map<String, MongoClient> clientCache;
    private final AmazonS3 amazonS3;

    public DocDBRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), new HashMap<>());
    }

    @VisibleForTesting
    protected DocDBRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, Map<String, MongoClient> clientCache)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.clientCache = clientCache;
    }

    private MongoClient getOrCreateClient(ReadRecordsRequest request)
    {
        String catalog = request.getCatalogName();
        MongoClient result = clientCache.get(catalog);
        if (result == null) {
            result = MongoClients.create(System.getenv(catalog));
            clientCache.put(catalog, result);
        }
        return result;
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        TableName tableName = recordsRequest.getTableName();
        Map<String, ValueSet> constraintSummary = recordsRequest.getConstraints().getSummary();

        MongoClient client = getOrCreateClient(recordsRequest);
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
