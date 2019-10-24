## Example Athena Connector

This module is meant to serve as a guided example for writing and deploying your own connector to enable Athena to query a custom source. The goal with this guided tutorial is to help you understand the development process and point out capabilities. Out of necessity some of the examples are rather contrived and make use of hard coded schemas to separate learning how to write a connector from learning how to interface with the target systems you will inevitably want to federate to. 

## What is a 'Connector'?

A 'Connector' is a piece of code that can translate between your target data source and Athena. Today this code is expected to run in an AWS Lambda function but in the future we hope to offer more options. You can think of a connector as an extension of Athena's query engine. Athena will delegate portions of the federated query plan to your connector. More specifically:

1. Your connector must provide a source of meta-data for Athena to get schema information about what databases, tables, and columns your connector has. This is done by building and deploying a lambda function that extends com.amazonaws.athena.connector.lambda.handlers.MetadataHandler in the athena-federation-sdk module. 
2. Your connector must provide a way for Athena to read the data stored in your tables. This is done by building and deploying a lambda function that extends com.amazonaws.athena.connector.lambda.handlers.RecordHandler in the athena-federation-sdk module. 

Alternatively, you can deploy a single Lambda function which combines the two above requirements by using com.amazonaws.athena.connector.lambda.handlers.CompositeHandler or com.amazonaws.athena.connector.lambda.handlers.UnifiedHandler. While breaking this into two separate Lambda functions allows you to independently control the cost and timeout of your Lambda functions, using a single Lambda function can be simpler and higher performance due to less cold start.

In the next section we take a closer look at the methosd we must implement on the MetadataHandler and RecordHandler.

### MetadataHandler Details

Lets take a closer look at what is required for a MetadataHandler. Below we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization. The abstract class we are extending takes care of all the Lambda interface bits and delegates on the discrete operations that are relevant to the task at hand, querying our new data source.

```java
public class MyMetadataHandler extends MetadataHandler
{
    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) {}

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {}

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     *             1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     *             2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     */
    @Override
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) {}

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @return A GetTableLayoutResponse which primarily contains:
     *             1. An Apache Arrow Block with 0 or more partitions to read. 0 partitions implies there are 0 rows to read.
     *             2. Set<String> of partition column names which should correspond to columns in your Apache Arrow Block.
     * @note Partitions are opaque to Amazon Athena in that it does not understand their contents, just that it must call
     * doGetSplits(...) for each partition you return in order to determine which reads to perform and if those reads
     * can be parallelized. This means the contents of this response are more for you than they are for Athena.
     */
    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator allocator, GetTableLayoutRequest request) {}

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     *             1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     *             2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {}
}
```

You can find example MetadataHandlers by looking at some of the connectors in the repository. athena-cloudwatch and athena-tpcds are fairly easy to follow along with.

Alternatively, if you wish to use AWS Glue DataCatalog as the authrotiative (or suplimental) source of meta-data for your connector you can extend com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler instead of com.amazonaws.athena.connector.lambda.handlers.MetadataHandler. GlueMetadataHandler comes with implementations for doListSchemas(...), doListTables(...), and doGetTable(...) leaving you to implemented only 2 methods. The Amazon Athena DocumentDB Connector in the athena-docdb module is an example of using GlueMetadataHandler.

### RecordHandler Details

Lets take a closer look at what is required for a RecordHandler. Below we have the basic functions we need to implement when using the Amazon Athena Query Federation SDK's MetadataHandler to satisfy the boiler plate work of serialization and initialization. The abstract class we are extending takes care of all the Lambda interface bits and delegates on the discrete operations that are relevant to the task at hand, querying our new data source.

```java
public class MyRecordHandler
        extends RecordHandler
{
    /**
     * Used to read the row data associated with the provided Split.
     * @param constraints A ConstraintEvaluator capable of applying constraints form the query that request this read.
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     *                The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     *       ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(ConstraintEvaluator constraints, BlockSpiller spiller, ReadRecordsRequest recordsRequest){}
}
```

## How To Build & Deploy

You can use any IDE or even just comman line editor to write your connector. The below steps show you how to use an AWS Cloud9 IDE running on EC2 to get started but most of the steps are applicable to any linux based development machine.


#### Step 1: Create your Cloud9 Instance

1. Open the AWS Console and navigate to the [Cloud9 Service or Click Here](https://console.aws.amazon.com/cloud9/)
2. Click 'Create Environment' and follow the steps to create a new instance using a new EC2 Instance (we recommeded m4.large) running Amazon Linux. 


#### Step 2: Download The SDK + Connectors

1. At your Cloud9 terminal run `git clone https://github.com/awslabs/aws-athena-query-federation.git` to get a copy of the Amazon Athena Query Federation SDK, Connector Suite, and Example Connector.

#### Step 3: Install Development Tools (Pre-Requisites)

This step may be optional if you are working on a development machine that already has Apache maven, the aws cli, and the aws sam build tool for Serverless Applications. If not, you can run  the `./prepare_dev_env.sh` script in the athena-example module director.

```bash

#Install Maven and Java 8
sudo wget https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
sudo yum -y install java-1.8.0-openjdk-devel
sudo update-alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
sudo update-alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac

# If the above update-alternatives doesn't work and you don't know your path try 
#  sudo update-alternatives --config java
#  sudo update-alternatives --config javac


#Install HomeBrew so we can install aws cli and aws-sam
sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)"

#Prepare the Brew environment
test -d ~/.linuxbrew && eval $(~/.linuxbrew/bin/brew shellenv)
test -d /home/linuxbrew/.linuxbrew && eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)
test -r ~/.bash_profile && echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.bash_profile
echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.profile

#Install the latest aws cli and sam tools (run these one at a time)
brew tap aws/tap
brew install awscli
brew install aws-sam-cli

```

Now run `mvn clean install` from the athena-federation-sdk directory within the github project you checked out earlier.

### Step 4: Write The Code

1. Create an s3 bucket, that we can use for spill and to upload some sample data using the following command `aws s3 mb s3://BUCKET_NAME` but be sure to put your actual bucket name in the command and taht you pick something that is unlikely to already exist.
2. (If using Cloud9) Navigate to the aws-athena-query-federation/athena-example folder on the left nav. This is the code you extracted back in Step 2.
3. Complete the TODOs in ExampleMetadataHandler
4. Complete the TODOs in ExampleRecordHandler
5. Run the following command from the aws-athena-query-federation/athena-example directory to ensure your connector is valid.  `mvn clean install`
6. Upload our sample data by running the following command from aws-athena-query-federation/athena-example directory.  `aws s3 cp ./sample_data.csv s3://BUCKET_NAME/2017/11/1/sample_data.csv`

### Step 5: Setup Our Deployment Configs

1. Create a new file in athena-example called `sar_bucket_policy.json` with the below S3 policy which we will use to grant Serverless App Repo permission to our new connector. Be sure to put in the name of your bucket, we'll create that bucket in a later step. Pick a unique name thats unlikely to be taken already.

  ```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service":  "serverlessrepo.amazonaws.com"
            },
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::BUCKET_NAME/*"
        }
    ]
}
```

2. Edit athena-example.yaml to have appropriate policies for you usecase (e.g. For this tutorial the below policy for spilling to S3 is sufficient.)

```yaml
      Policies:
        #S3CrudPolicy allows our connector to spill large responses to S3. You can optionally replace this pre-made policy
        #with one that is more restrictive and can only 'put' but not read,delete, or overwrite files.
        - S3CrudPolicy:
            BucketName: !Ref SpillBucket
        - S3CrudPolicy:
            BucketName: !Ref DataBucket
```

3. Grant Serverless Application Repository read access to that bucket so that it can get a copy of our connector when we publish it for use by other people in our AWS Account. Run 
`aws s3api put-bucket-policy --bucket BUCKET_NAME --policy  file://sar_bucket_policy.json` but be sure to put your actual bucket name in the command.

### Step 6: Build and Package Your New Connector

1. Run `mvn clean install` in the aws-athena-query-federation/athena-example directory
2. Run `sam package --template-file athena-example.yaml --output-template-file packaged.yaml --s3-bucket BUCKET_NAME` from the aws-athena-query-federation/athena-example directory but be sure to put your actual bucket name in the command.

### Step 7: Deploy Your New Connector

We have two options for deploying our connector: directly to Lambda or via Serverless Application Repository. We'll do both below.

*Publish Your Connector To Serverless Application Repository*

Run `sam publish --template packaged.yaml --region AWS_REGION` to publish the connector to the AWS Serverless Application Repository as a private application available to anyone in your AWS Account. This allows 1-click deployments of the connector. You can optionally make it public later. 

Then you can navigate to [Serverless Application Repository](https://console.aws.amazon.com/serverlessrepo/) to search for your application and deploy it before using it from Athena.
 

*Publish Your Connector Directly To Lambda*

In order to deploy our Lambda function we must first ensure that we have a role defined that we'd like our Lambda function to use. Below we have an example IAM policy for a Role that supports Spilling to S3 as well as reading information from Cloudwatch. We frequently use Cloudwatch as an example data source since it's API is relatively easily to understand and use in a connector.

1. Create our new role for our Lambda function by going to the [AWS IAM Console](https://console.aws.amazon.com/iam/) and creating a role named 'athena-example-role' using the below policy json to create a policy for that role. Take note of the role arn as we will need it in the next step.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::DATA_BUCKET/*",
                "arn:aws:s3:::DATA_BUCKET",
                "arn:aws:s3:::SPILL_BUCKET/*"
            ]
        }
    ]
}
```

2. Run the following command from the athena-example directory to create our Lambda function but be sure to insert the ARN of the role you created in the previous step as well as the catalog name (aka function name) you'd like to query in Athena.
```bash
aws lambda create-function \
    --function-name <catalog_name>\
    --runtime java8 \
    --timeout 900 \
    --memory-size 3008 \
    --zip-file fileb://target/athena-example-1.0.jar\
    --handler com.amazonaws.connectors.athena.example.ExampleCompositeHandler \
    --role <ROLE_ARN> \
    --environment Variables={spill_bucket=<SPILL_BUCKET>}
```

### Step 8: Run a Query!

Ok, now we are ready to try running some queries using our new connector. Some good examples to try include (be sure to put in your actual database and table names):

`select * from "lambda:<catalog>".schema1.table1 where year=2017 and month=11 and day=1;`

`select transaction.completed, count(*) from "lambda:<catalog>".schema1.table1 where year=2017 and month=11 and day=1 group by transaction.completed;`






