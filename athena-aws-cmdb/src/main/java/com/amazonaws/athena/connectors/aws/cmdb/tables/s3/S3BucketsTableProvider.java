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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Owner;

/**
 * Maps your S3 Objects to a table.
 */
public class S3BucketsTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private S3Client amazonS3;

    public S3BucketsTableProvider(S3Client amazonS3)
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
        return new TableName(getSchema(), "buckets");
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
        for (Bucket next : amazonS3.listBuckets().buckets()) {
            toRow(next, spiller);
        }
    }

    /**
     * Maps a DBInstance into a row in our Apache Arrow response block(s).
     *
     * @param bucket The S3 Bucket to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void toRow(Bucket bucket,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;
            matched &= block.offerValue("bucket_name", row, bucket.name());
            matched &= block.offerValue("create_date", row, bucket.creationDate());

            GetBucketAclResponse response = amazonS3.getBucketAcl(GetBucketAclRequest.builder().bucket(bucket.name()).build());

            Owner owner = response.owner();
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
                .addDateMilliField("create_date")
                .addStringField("owner_name")
                .addStringField("owner_id")
                .addMetadata("bucket_name", "The name of the bucket that this object is in.")
                .addMetadata("create_date", "The time the bucket was created.")
                .addMetadata("owner_name", "The owner name of the object.")
                .addMetadata("owner_id", "The owner_id of the object.")
                .build();
    }
}
