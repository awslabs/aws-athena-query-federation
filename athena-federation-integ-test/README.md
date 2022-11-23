# Integration-Test Framework

The Integration-Test framework provides end-to-end testing capabilities, and is available
to all lambda connectors developed using the Athena Federation SDK.

## How It Works

In order to test the connectors end-to-end, several infrastructure resources need to be
provisioned and deployed (e.g. DB instance, Lambda function, etc...) The framework accomplishes
that by allowing AWS CloudFormation to manage the infrastructure resources. All an
integration-test writer needs to do is provide an implementation for a handful of functions,
and the Integration-Test framework will do the rest.

The framework provides the following benefits:
* Automatically provisions all infrastructure resources prior to testing, and de-provisions
them immediately after.
* Provides a set of public APIs that can be used to send queries via Athena using the lambda
connector.

## Writing Integration Tests

This section explains the steps necessary to create integration tests using the
Integration-Test framework. For actual code examples, see the DynamoDB connector
([DynamoDbIntegTest](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-dynamodb/src/test/java/com/amazonaws/athena/connectors/dynamodb/DynamoDbIntegTest.java)),
and the Redshift (JDBC) connector
([RedshiftIntegTest](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-jdbc/src/test/java/com/amazonaws/connectors/athena/jdbc/integ/RedshiftIntegTest.java)).

### Dependencies

Add the Integration-Test module (athena-federation-integ-test) as a `test` dependency in the specific connector's
**pom.xml** file, using the current version of the Athena Federation SDK (see below). The current version can be found
in most **pom.xml** files (e.g.
[athena-federation-sdk module - pom.xml](https://github.com/awslabs/aws-athena-query-federation/blob/master/athena-federation-sdk/pom.xml#L15-L16)).

```xml
        <dependency>
            <groupId>com.amazonaws</groupId>
            <artifactId>athena-federation-integ-test</artifactId>
            <version>Current version of the SDK (e.g. 2022.47.1)</version>
            <scope>test</scope>
        </dependency>
```

### Naming Convention

All integration-test classes need to adhere to the naming convention `<class>IntegTest`
(e.g. `public class DynamoDbIntegTest`). The same goes for the integration-tests themselves.

### Writing Integration Tests

Import the `@Test` annotation from `org.testng.annotations` and extend the Integration-Test
class (`IntegrationTestBase`):

```java
import org.testng.annotations.Test;

public class MyConnectorIntegTest extends IntegrationTestBase
{
    @Test
    public void exampleIntegTest()
    {
        //...
    }
}
```

Provide implementation for the following 4 abstract methods in the test class:

```java
    /**
     * Must be overridden in the extending class to setup the DB table (i.e. insert rows into table, etc...)
     */
    protected abstract void setUpTableData();

    /**
     * Must be overridden in the extending class (can be a no-op) to create a connector-specific CloudFormation stack
     * resource (e.g. DB table) using AWS CDK.
     * @param stack The current CloudFormation stack.
     */
    protected abstract void setUpStackData(final Stack stack);

    /**
     * Must be overridden in the extending class (can be a no-op) to set the lambda function's environment variables
     * key-value pairs (e.g. "connection_string":"redshift://jdbc:redshift://..."). See individual connector for the
     * expected environment variables. This method is intended to supplement the test-config.json file environment_vars
     * attribute (see below) for cases where the environment variable cannot be hardcoded.
     */
    protected abstract void setConnectorEnvironmentVars(final Map<String, String> environmentVars);

    /**
     * Must be overridden in the extending class to get the lambda function's IAM access policy. The latter sets up
     * access to multiple connector-specific AWS services (e.g. DynamoDB, Elasticsearch etc...)
     * @return A policy document object.
     */
    protected abstract Optional<PolicyDocument> getConnectorAccessPolicy();
```

### Test Configuration

The Integration-Test framework uses several configurable attributes to set up the test resources (e.g. a spill bucket,
Athena work-group, etc...) Those attributes must be placed in the connectors' `test-config.json` JSON file available
in each connector's **etc** directory. The following is an example of a test configuration file used for the Redshift
integration tests:
```json
{
  "athena_work_group" : "FederationIntegrationTests",
  "secrets_manager_secret" : "redshift-integ1",
  "environment_vars" : {
    "spill_bucket" : "athena-results",
    "spill_prefix" : "athena-spill",
    "disable_spill_encryption" : "false",
    "spill_put_request_headers": ""
  },
  "vpc_configuration" : {
    "vpc_id": "vpc-569cdc2c",
    "security_group_id": "sg-2bc8117a",
    "subnet_ids": ["subnet-a3017a9d", "subnet-bb894ef6", "subnet-7b6f5f27", "subnet-d55361fb",
      "subnet-da5db3d4", "subnet-88d6e2ef"],
    "availability_zones": ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1e",
      "us-ease-1f"]
  },
  "user_settings" : {
    "redshift_db_name": "public",
    "redshift_db_port": "5439",
    "redshift_table_movies": "movies",
    "redshift_table_bday": "bday"
  }
}
```
General attributes needed for the tests' execution:
* **athena_work_group** - The Athena Workgroup used for running integration tests (default:
  `FederationIntegrationTests`).
* **secrets_manager_secret** - Secret name used to retrieve user credentials from SecretsManager.

Since secret credentials may be needed when creating resources specific to the tests' execution (e.g. DB cluster), the
Integration-Test framework provides the following public API allowing access to the SecretsManager secret credentials:
```java
    /**
     * Public accessor for the SecretsManager credentials obtained using the secrets_manager_secret attribute entered
     * in the config file.
     * @return Optional SecretsManager credentials object.
     */
    public Optional<SecretsManagerCredentials> getSecretCredentials()
    {
        return secretCredentials;
    }
```
To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.

**Environment variables** - Parameters used by the connectors' internal logic:
* **spill_bucket** - The S3 bucket used for spilling excess data.
* **spill_prefix** - The prefix within the S3 spill bucket (default: `athena-spill`).
* **spill_put_request_headers** - (Optional) JSON encoded map of request headers and values for the s3 putObject request used for spilling. Example: `{"x-amz-server-side-encryption" : "AES256"}`. For more possible headers see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
* **disable_spill_encryption** - If set to `true` encryption for spilled data is disabled (default: `false`).

**VPC configuration** (Optional) - Parameters needed to configure resources within a VPC (e.g. DB cluster):
* **vpc_id** - The VPC Id (e.g. `"vpc_id": "vpc-xxx"`).
* **security_group_id** - The Security Group Id (e.g. `"security_group_id": "sg-xxx"`).
* **subnet_ids** - A list consisting of at least one Subnet Id (e.g. `"subnet_ids": ["subnet-xxx1", "subnet-xxx2"]`).
* **availability_zones** - A list consisting of at least one AZ (e.g. `"availability_zones": ["us-east-1a", "us-east-1b"]`).

The framework uses the aforementioned attributes to configure the connector. In order for the latter to be able to connect
to the data source, however, the same VPC configuration must be set when provisioning the DB instance. To that end, the
Integration-Test framework provides the following public API allowing access to the VPC attributes:
```java
    /**
     * Public accessor for the VPC attributes used in generating the lambda function.
     * @return Optional VPC attributes object.
     */
    public Optional<ConnectorVpcAttributes> getVpcAttributes()
```

**User settings**: (Optional)
User customizable Map that contains user-specific attributes (e.g. `"user_settings": {"redshift_table_movies": "movies"}`). Because the Map
is constructed from a JSON structure and returned as Map<String, Object>, it can contain different type of attributes
ranging from a single value, a list of values, to even a nested structure. the Integration-Test framework provides the
following public API allowing access to the `user_settings` attribute:
```java
    /**
     * Public accessor for the user_settings attribute (stored in the test-config.json file) that are customizable to
     * any user-specific purpose.
     * @return Optional Map(String, Object) containing all the user attributes as defined in the test configuration file,
     * or an empty Optional if the user_settings attribute does not exist in the file.
     */
    public Optional<Map> getUserSettings()
```

### Integration-Test Public APIs

The Integration-Test framework provides the following 5 public APIs that can be used to send DB
queries as part of the tests' execution:

```java
    /**
     * Gets the name of the lambda function generated by the Integration-Test framework.
     * @return The name of the lambda function.
     */
    public String getLambdaFunctionName()

    /**
     * Uses the listDatabases Athena API to list databases for the data source utilizing the lambda function.
     * @return a list of database names.
     */
    public List<String> listDatabases()

    /**
     * Uses the startQueryExecution Athena API to process a "show tables" query utilizing the lambda function.
     * @param databaseName The name of the database.
     * @return A list of database table names.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public List<String> listTables(String databaseName)

    /**
     * Uses the startQueryExecution Athena API to process a "describe table" query utilizing the lambda function.
     * @param databaseName The name of the database.
     * @param tableName The name of the database table.
     * @return A Map of the table column names and their associated types.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public Map<String, String> describeTable(String databaseName, String tableName)

    /**
     * Sends a DB query via Athena and returns the query results.
     * @param query - The query string to be processed by Athena.
     * @return The query results object containing the metadata and row information.
     * @throws RuntimeException The Query is cancelled or has failed.
     */
    public GetQueryResultsResult startQueryExecution(String query)
```

## Running Integration Tests

This section explains the steps necessary to run the integration tests for a connector
locally from the terminal.

### Environment Setup

The following commands should be sent after cloning the Federation GitHub repository for
the first time, and each time the connector's code changes:

1. From the **athena-federation-sdk** dir, run `mvn clean install` if you haven't done so already.
2. From the **athena-federation-integ-test** dir, run `mvn clean install` if you haven't done so already
   (**Note: failure to follow this step will result in compilation errors**).
3. From your connector's dir, run `mvn clean install`.
4. Export the IAM credentials for the AWS account used for testing purposes.
5. Package the connector (from the connector's directory):
`sam package --template-file <connector.yaml> --output-template-file packaged.yaml
--s3-bucket <s3-bucket> --region <region> --force-upload`

### Running Integration Tests

The following command will trigger the integration tests: `mvn failsafe:integration-test`

If run from the root directory, the command will execute the integration tests for all connectors.
Likewise, if run from a specific connector's directory, it will trigger the integration tests
for the specific connector.
