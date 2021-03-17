# Amazon Athena UDF Connector

This connector extends Amazon Athena's capability by adding customizable UDFs via Lambda.

**To enable this Preview feature you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to use a UDF or SageMaker inference from that workgroup.**

## Supported UDFs

1. "compress": Compresses a String

Example query:

`USING FUNCTION compress(col1 VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = '<lambda name>') SELECT compress('StringToBeCompressed');`

This would return result 'eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w=='.

2. "decompress": Decompresses a String

`USING FUNCTION decompress(col1 VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = '<lambda name>') SELECT decompress('eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w==');`

This would return result 'StringToBeCompressed'.

3. "encrypt": encrypt the data with a data key stored in AWS Secrets Manager

Before testing this query, you would need to create a secret in AWS Secrets Manager. Make sure to use "DefaultEncryptionKey". If you choose to use your KMS key, you would need to update ./athena-udfs.yaml to allow access to your KMS key. Remove all the json brackets and store a base64 encoded string as data key. Sample data is like `AQIDBAUGBwgJAAECAwQFBg==`. 

Example query:

`USING FUNCTION encrypt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = '<lambda name>') SELECT encrypt('plaintext', 'my_secret_name');`

3. "decrypt": decrypt the data with a data key stored in AWS Secrets Manager

Example query:

`USING FUNCTION decyprt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = '<lambda name>') SELECT decyprt('tEgyixKs1d0RsnL51ypMgg==', 'my_secret_name');`


### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-udfs dir, run `mvn clean install`.
3. From the athena-udfs dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-udfs` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
4. Try using your UDF(s) in a query.

## License

This project is licensed under the Apache-2.0 License.