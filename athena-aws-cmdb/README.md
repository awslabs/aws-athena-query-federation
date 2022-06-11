# Amazon Athena AWS CMDB Connector

This connector enables Amazon Athena to communicate with various AWS Services, making your AWS Resource inventory accessible via SQL. 

**Athena Federated Queries are now enabled as GA in us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1, ap-south-1, us-west-1, ap-southeast-1, ap-southeast-2, eu-west-2, ap-northeast-2, eu-west-3, ca-central-1, sa-east-1, and eu-central-1. To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.**

## Usage

### Parameters

The Athena AWS CMDB Connector provides several configuration options via Lambda environment variables. More detail on the available parameters can be found below.

1. **spill_bucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
2. **spill_prefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-federation-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
3. **spill_put_request_headers** - (Optional) This is a JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
4. **kms_key_id** - (Optional) By default any data that is spilled to S3 is encrypted using AES-GCM and a randomly generated key. Setting a KMS Key ID allows your Lambda function to use KMS for key generation for a stronger source of encryption keys. (e.g. a7e63k4b-8loc-40db-a2a1-4d0en2cd8331)
5. **disable_spill_encryption** - (Optional) Defaults to False so that any data that is spilled to S3 is encrypted using AES-GCM either with a randomly generated key or using KMS to generate keys. Setting this to false will disable spill encryption. You may wish to disable this for improved performance, especially if your spill location in S3 uses S3 Server Side Encryption. (e.g. True or False)
6. **default_ec2_image_owner** - (Optional) When set, this controls the default ec2 image (aka AMI) owner used to filter AMIs. When this isn't set and your query against the ec2 images table does not include a filter for owner you will get a large number of results since the response will include all public images.

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
2. **emr** - This database contains EMR related resources, including:
  * **emr_clusters** - Contains details of your EMR Clusters.
3. **rds** - This database contains RDS related resources, including:
  * **rds_instances** - Contains details of your RDS Instances.
4. **s3** - This database contains RDS related resources, including:
  * **buckets** - Contains details of your S3 buckets.
  * **objects** - Contains details of your S3 Objects (excludes their contents).
  
### Required Permissions

Review the "Policies" section of the athena-aws-cmdb.yaml file for full details on the IAM Policies required by this connector. A brief summary is below.

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
1. EC2 Describe - The connector uses this access to describe your EC2 Instances, Security Groups, VPCs, EBS Volumes, etc...
1. EMR Describe / List - The connector uses this access to describe your EMR Clusters.
1. RDS Describe - The connector uses this access to describe your RDS Instances.
1. S3 List - The connector uses this access to list your buckets and objects.
1. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-aws-cmdb dir, run `mvn clean install`.
3. From the athena-aws-cmdb dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-aws-cmdb` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
4. Try running a query like the one below in Athena: 
```sql
select * from "lambda:<CATALOG_NAME>".ec2.ec2_instances limit 100
```

## Performance

The Athena AWS CMDB Connector does not current support parallel scans. Predicate Pushdown is performed within the Lambda function and where possible partial predicates are pushed to the services being queried. For example, a query for the details of a specific EC2 Instance will turn into a targeted describe of that specific instance id against the EC2 API. 

## License

This project is licensed under the Apache-2.0 License.
