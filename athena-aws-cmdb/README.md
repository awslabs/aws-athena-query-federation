# Amazon Athena AWS CMDB Connector

This connector enables Amazon Athena to communicate with various AWS Services, making your AWS Resource inventory accessible via SQL. 

## Usage

### Parameters

The Athena AWS CMDB Connector provides several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
4. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GMC either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)

### Databases & Tables

The Athena AWS CMDB Connector makes the following databases and tables available for querying your AWS Resource Inventory. For more information on the columns available in each table, try running a 'describe database.table' from the Athena Console or API.

1. **ec2** - This database contains EC2 related resources, including: 
  * **ebs_volumes** - Contains details of you EBS volumes.
  * **ec2_instances** - Contains details of your EC2 Instances.
  * **ec2_images** - Contains details of your EC2 Instance images.
  * **routing_tables** - Contains details of your VPC Routing Tables.
  * **security_groups** - Contains details of your Security Groups.
  * **subnets** - Contains details of your VPC Subnets.
  * **vpcs** - Contains details of your VPCs.
1. **emr** - This database contains EMR related resources, including:
  * **emr_clusters** - Contains details of your EMR Clusters.
1. **rds** - This database contains RDS related resources, including:
  * **rds_instances** - Contains details of your RDS Instances.

### Required Permissions

Review the "Policies" section of the athena-aws-cmdb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
1. EC2 Describe - The connector uses this access to describe your EC2 Instances, Security Groups, VPCs, EBS Volumes, etc...
1. EMR Describe / List - The connector uses this access to describe your EMR Clusters.
1. RDS Describe - The connector uses this access to describe your RDS Instances.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-aws-cmdb dir, run `mvn clean install`.
3. From the athena-aws-cmdb dir, run `sam package --template-file athena-aws-cmdb.yaml --output-template-file packaged.yaml --s3-bucket <your_lambda_source_bucket_name>`
4. Deploy your application using either Server-less Application Repository or Lambda directly. Instructions below.

For Server-less Application Repository, run `sam publish --template packaged.yaml --region <aws_region>` from the athena-cloudwatch directory and then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

For a direct deployment to Lambda, you can use the below command from the athena-aws-cmdb directory. Be sure to insert your S3 Bucket and Role ARN as indicated.

```bash
aws lambda create-function \
        --function-name athena-aws-cmdb \
        --runtime java8 \
        --timeout 900 \
        --memory-size 3008 \
        --zip-file fileb://target/athena-aws-cmdb-1.0.jar \
        --handler com.amazonaws.athena.connectors.aws.cmdb.AwsCmdbCompositeHandler \
        --role "!!!<LAMBDA_ROLE_ARN>!!!" \
        --environment Variables={spill_bucket=<!!BUCKET_NAME!!>}
```

## Performance

The Athena AWS CMDB Connector does not current support parallel scans. Predicate Pushdown is performed within the Lambda function and where possible partial predicates are pushed to the services being queried. For example, a query for the details of a specific EC2 Instance will turn into a targeted describe of that specific instance id against the EC2 API. 

## License

This project is licensed under the Apache-2.0 License.