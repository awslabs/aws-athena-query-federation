# Amazon Athena UDF Connector

This connector extends Amazon Athena's capability by adding customizable UDFs via Lambda.

## Supported UDFs

1. "compress": Compresses a String

Example query:

`USING EXTERNAL FUNCTION compress(col1 VARCHAR) RETURNS VARCHAR LAMBDA '<lambda name>' SELECT compress('StringToBeCompressed');`

This would return result 'eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w=='.

2. "decompress": Decompresses a String

`USING EXTERNAL FUNCTION decompress(col1 VARCHAR) RETURNS VARCHAR LAMBDA '<lambda name>' SELECT decompress('eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w==');`

This would return result 'StringToBeCompressed'.

3. "encrypt": encrypt the data with a data key stored in AWS Secrets Manager*

Before testing this query, you would need to create a secret in AWS Secrets Manager. Make sure to use "DefaultEncryptionKey". If you choose to use your KMS key, you would need to update ./athena-udfs.yaml to allow access to your KMS key. Remove all the json brackets and store a base64 encoded string as data key. Sample data is like `i5YnyBO4gJKWuIQ+gjuJjcJ/5kUph9pmYFUbW7zf3PE=`. 

Example query:

`USING EXTERNAL FUNCTION encrypt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR LAMBDA '<lambda name>' SELECT encrypt('plaintext', 'my_secret_name');`

4. "decrypt": decrypt the data with a data key stored in AWS Secrets Manager*

Example query:

`USING EXTERNAL FUNCTION decrypt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR LAMBDA '<lambda name>' SELECT decrypt('G/VP2sbMb7d4zE2HVl2XkiB5xUHpszlEjccEBsTVji209IaCjg==', 'my_secret_name');`

*To use the Athena Federated Query feature with AWS Secrets Manager, the VPC connected to your Lambda function should have [internet access](https://aws.amazon.com/premiumsupport/knowledge-center/internet-access-lambda-function/) or a [VPC endpoint](https://docs.aws.amazon.com/secretsmanager/latest/userguide/vpc-endpoint-overview.html#vpc-endpoint-create) to connect to Secrets Manager.

## AWS built UDFs
For an example that uses UDFs with Athena to translate and analyze text, see the AWS
                                    Machine Learning Blog article <a href="http://aws.amazon.com/blogs/machine-learning/translate-and-analyze-text-using-sql-functions-with-amazon-athena-amazon-translate-and-amazon-comprehend/" rel="noopener noreferrer" target="_blank"><span>Translate and analyze text using SQL functions with Amazon Athena, Amazon Translate,
                                          and Amazon Comprehend</span></a>, or watch the <a href="#udf-videos-xlate">video</a>

### Repositories for AWS built UDFs

1. https://github.com/aws-samples/aws-athena-udfs-textanalytics

## Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-udfs dir, run `mvn clean install`.
3. From the athena-udfs dir, run  `sam deploy --template-file athena-udfs.yaml -g` and follow the guided prompt to synthesize your CloudFormation template and create your IAM policies and Lambda function. 
4. Try using your UDF(s) in a query.

## Migrating To V2
This UDF includes a sample encryption/decryption method to showcase the benefits of integrating UDFs into your queries. If you were using the prior version of this UDF with AES-based encryption and wish to transition to the new version, please follow these steps:

1. Deploy the new connector with new name (say v2)  as shown in previous example. 
2. Use the previously deployed connector to decrypt, and the new one to encrypt:

```
USING    EXTERNAL FUNCTION decrypt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR LAMBDA 'athena_udf_v1',
         EXTERNAL FUNCTION encrypt(col VARCHAR, secretName VARCHAR) RETURNS VARCHAR LAMBDA 'athena_udf_v2'
SELECT   encrypt(t.plaintext, 'SOME_SECRET')
         FROM (SELECT decrypt('PREVIOUSLY_ENCRYPTED_MESSAGE', 'SOME_SECRET') as plaintext) as t
```

## License

This project is licensed under the Apache-2.0 License.
