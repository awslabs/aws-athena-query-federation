/*-
 * #%L
 * athena-aws-cmdb
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
package com.amazonaws.athena.connectors.aws.cmdb.tables.s3;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Maps your S3 Objects to a table.
 */
public class S3ObjectsTableProvider
        implements TableProvider
{
    private static final int MAX_KEYS = 1000;
    private static final Schema SCHEMA;
    private S3Client amazonS3;

    public S3ObjectsTableProvider(S3Client amazonS3)
    {
        this.amazonS3 = amazonS3;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "s3";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "objects");
    }

    /**
     * @See TableProvider
     */
    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    /**
     * Calls DescribeDBInstances on the AWS RDS Client returning all DB Instances that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific DB Instance) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        ValueSet bucketConstraint = recordsRequest.getConstraints().getSummary().get("bucket_name");
        String bucket;
        if (bucketConstraint != null && bucketConstraint.isSingleValue()) {
            bucket = bucketConstraint.getSingleValue().toString();
        }
        else {
            throw new IllegalArgumentException("Queries against the objects table must filter on a single bucket " +
                    "(e.g. where bucket_name='my_bucket'.");
        }

        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucket).maxKeys(MAX_KEYS).build();
        ListObjectsV2Response response;
        do {
            response = amazonS3.listObjectsV2(req);
            for (S3Object s3Object : response.contents()) {
                toRow(s3Object, spiller, bucket);
            }
            req = req.toBuilder().continuationToken(response.nextContinuationToken()).build();
        }
        while (response.isTruncated() && queryStatusChecker.isQueryRunning());
    }

    /**
     * Maps a DBInstance into a row in our Apache Arrow response block(s).
     *
     * @param s3Object The S3 object to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @param bucket The name of the S3 bucket
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void toRow(S3Object s3Object,
            BlockSpiller spiller,
            String bucket)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;
            matched &= block.offerValue("bucket_name", row, bucket);
            matched &= block.offerValue("e_tag", row, s3Object.eTag());
            matched &= block.offerValue("key", row, s3Object.key());
            matched &= block.offerValue("bytes", row, s3Object.size());
            matched &= block.offerValue("storage_class", row, s3Object.storageClassAsString());
            matched &= block.offerValue("last_modified", row, s3Object.lastModified());

            Owner owner = s3Object.owner();
            if (owner != null) {
                matched &= block.offerValue("owner_name", row, owner.displayName());
                matched &= block.offerValue("owner_id", row, owner.id());
            }

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("bucket_name")
                .addStringField("key")
                .addStringField("e_tag")
                .addBigIntField("bytes")
                .addStringField("storage_class")
                .addDateMilliField("last_modified")
                .addStringField("owner_name")
                .addStringField("owner_id")
                .addMetadata("bucket_name", "The name of the bucket that this object is in.")
                .addMetadata("key", "The key of the object.")
                .addMetadata("e_tag", "eTag of the Object.")
                .addMetadata("bytes", "The size of the object in bytes.")
                .addMetadata("storage_class", "The storage class of the object.")
                .addMetadata("last_modified", "The last time the object was modified.")
                .addMetadata("owner_name", "The owner name of the object.")
                .addMetadata("owner_id", "The owner_id of the object.")
                .build();
    }
}
