package com.amazonaws.connectors.athena.example;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.lang.String.format;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ExampleMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    private AmazonS3 amazonS3;

    public ExampleRecordHandler()
    {
        super(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), SOURCE_TYPE);
        this.amazonS3 = AmazonS3ClientBuilder.standard().build();
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param constraints A ConstraintEvaluator capable of applying constraints form the query that request this read.
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraints, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
            throws IOException
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());

        Split split = recordsRequest.getSplit();
        int splitYear = 0;
        int splitMonth = 0;
        int splitDay = 0;

        /**
         * TODO: Extract information about what we need to read from the split. If you are following the tutorial
         *  this is basically the partition column values for year, month, day.
         *
         splitYear = split.getPropertyAsInt("year");
         splitMonth = split.getPropertyAsInt("month");
         splitDay = split.getPropertyAsInt("day");
         *
         */

        //TODO: set this to the bucket you are using for the tutorial
        String dataBucket = "";
        String dataKey = format("%s/%s/%s/sample_data.csv", splitYear, splitMonth, splitDay);

        BufferedReader s3Reader = openS3File(dataBucket, dataKey);
        if (s3Reader == null) {
            //There is no data to read for this split.
            return;
        }

        //We read the transaction data line by line from our S3 object.
        String line;
        while ((line = s3Reader.readLine()) != null) {
            logger.info("readWithConstraint: processing line " + line);

            //The sample_data.csv file is structured as year,month,day,account_id,transaction.id,transaction.complete
            String[] lineParts = line.split(",");

            //We use the provided BlockSpiller to write our row data into the response. This utility is provided by
            //the Amazon Athena Query Federation SDK and automatically handles breaking the data into reasonably sized
            //chunks, encrypting it, and spilling to S3 if we've enabled these features.
            spiller.writeRows((Block block, int rowNum) -> {
                boolean rowMatched = true;

                int year = Integer.parseInt(lineParts[0]);
                int month = Integer.parseInt(lineParts[1]);
                int day = Integer.parseInt(lineParts[2]);
                String accountId = lineParts[3];
                int transactionId = Integer.parseInt(lineParts[4]);
                boolean transactionComplete = Boolean.parseBoolean(lineParts[5]);

                /**
                 * TODO: As an optional optimization, we can reduce the number of rows that we need to transmit and that
                 *  Athena needs to process (filter) by performing predicate push-down on in the connector using
                 *  the supplied constraint evaluator. Doing this in the connector can greatly improve performance
                 *  and also reduce your cost for the query. If this was a real data source we'd try to push the predicate
                 *  evaluation into the source for even better performance. Note that the SDK does not current support
                 *  pushing down predicates on nested/complex.
                 *
                 rowMatched &= constraints.apply("year", year) &&
                 constraints.apply("month", month) &&
                 constraints.apply("day", day) &&
                 constraints.apply("account_id", accountId);
                 *
                 */

                if (rowMatched) {
                    /**
                     * TODO: If our row matched all constraints write the data using the supplied Block.
                     *
                     block.offerValue("year", rowNum, year);
                     block.offerValue("month", rowNum, month);
                     block.offerValue("day", rowNum, day);

                     //For complex types like List and Struct, we can build a Map to conveniently set nested values
                     Map<String, Object> eventMap = new HashMap<>();
                     eventMap.put("id", transactionId);
                     eventMap.put("completed", transactionComplete);

                     block.offerComplexValue("transaction", rowNum, FieldResolver.DEFAULT, eventMap);
                     *
                     */

                    /**
                     * TODO: The account_id field is a sensitive field, so we'd like to mask it to the last 4 before
                     *  returning it to Athena. Note that this will mean you can only filter (where/having)
                     *  on the masked value from Athena.
                     *
                     String maskedAcctId = accountId.length() > 4 ? accountId.substring(accountId.length() - 4) : accountId;
                     block.offerValue("account_id", rowNum, maskedAcctId);
                     *
                     */
                }

                //We return the number of rows written for this invocation. In our case 1 or 0.
                return rowMatched ? 1 : 0;
            });
        }
    }

    /**
     * Helper function for checking the existence of and opening S3 Objects for read.
     */
    private BufferedReader openS3File(String bucket, String key)
    {
        logger.info("openS3File: opening file " + bucket + ":" + key);
        if (amazonS3.doesObjectExist(bucket, key)) {
            S3Object obj = amazonS3.getObject(bucket, key);
            logger.info("openS3File: opened file " + bucket + ":" + key);
            return new BufferedReader(new InputStreamReader(obj.getObjectContent()));
        }
        return null;
    }
}
