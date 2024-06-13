# Amazon Athena Lambda Clickhouse Connector

This connector enables Amazon Athena to access your ClickHouse databases. 

Official Public documentation has moved [here](https://docs.aws.amazon.com/athena/latest/ug/connectors-athena.html).

This README walks through the SAM CLI installation method (not Serverless Application Repository via AWS Console).

## 1. Download Athena Clickhouse Connector source and release repositories

Download latest Athena source
```
git clone https://github.com/awslabs/aws-athena-query-federation
cd athena-clickhouse
```

Download latest Athena Clickhouse JAR binary file. Browse to https://github.com/awslabs/aws-athena-query-federation/releases then choose latest release.  Note the version of the file may change over time.
```
wget https://github.com/awslabs/aws-athena-query-federation/releases/download/v2024.19.1/athena-clickhouse-2024.19.1.jar
```

## 2. Copy Athena Clickhouse Connector JAR file to Amazon S3 bucket since it is >= 50 MB local file upload limit

You **MUST** change the S3 bucket and prefix folder where the Connector JAR file will be stored for the subsequent SAM deployment step (4).

```
aws s3 cp --region us-east-2 athena-clickhouse-2024.19.1.jar  s3://my-athena-demo/code/
```

## 3. Validate Athena Clickhouse Connector as Serverless Cloudformation stack

```
sam validate --region us-east-2 --template-file athena-clickhouse.yaml
```

## 4. Deploy Athena Clickhouse Connector as Serverless Cloudformation stack

You can change the Lambda function configuration at deployment time and also once the stack has been deployed.  Parameters that **MUST** change are listed in section below.

Also, note that you **MUST** create and configure VPC endpoints for S3 (and *optionally* Secrets Manager) because the Athena connector's Lambda function will be deployed within a VPC.

Direct Configuration of Credentials in Connector's connection string:
```
sam deploy --guided --region us-east-2 --template-file athena-clickhouse.yaml --stack-name AthenaClickhouseConnectorStack --capabilities CAPABILITY_NAMED_IAM --parameter-overrides LambdaFunctionName=athenaclickhouseconnectorfunction DefaultConnectionString='clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?user=foo&password=bar&sslmode=none' DisableSpillEncryption=true SecretNamePrefix=AthenaClickhouse SpillBucket=my-athena-demo  SpillPrefix=athena-spill SecurityGroupIds=sg-ab9282d4 SubnetIds=subnet-bc1f0ac6,subnet-db9f40b0 LambdaS3CodeUriBucket=my-athena-demo LambdaS3CodeUriKey=code/athena-clickhouse-2024.19.1.jar
```

Indirect Configuration of Credentials in Connector's connection string using AWS Secrets Manager:
```
sam deploy --guided --region us-east-2 --template-file athena-clickhouse.yaml --stack-name AthenaClickhouseConnectorStack --capabilities CAPABILITY_NAMED_IAM --parameter-overrides LambdaFunctionName=athenaclickhouseconnectorfunction DefaultConnectionString='clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?${AthenaClickhouse}&sslmode=none' DisableSpillEncryption=true SecretNamePrefix=AthenaClickhouse SpillBucket=my-athena-demo  SpillPrefix=athena-spill SecurityGroupIds=sg-ab9282d4 SubnetIds=subnet-bc1f0ac6,subnet-db9f40b0 LambdaS3CodeUriBucket=my-athena-demo LambdaS3CodeUriKey=code/athena-clickhouse-2024.19.1.jar
```
### References

**Parameters** listed below.  You **MUST** change the `DefaultConnectionString`, `SpillBucket`, `SpillPrefix`, `SecurityGroupIds`, `SubnetIds`, `LambdaS3CodeUriBucket`, and `LambdaS3CodeUriKey`.  

Also, note that there are `DefaultConnectionString` differences depending on whether you directly configure within the URL or indirectly using AWS Secrets Manager.

If you decide to directly configure credentials in the URL, make sure that the URL contains parameters for the `user` and `password`.

If you decide to indirectly configure credentials using AWS Secrets Manager, make sure that the Secret contains parameters for the `username` and `password`.  And make sure that you use single quote when referencing it in the DefaultConnectionString on the Terminal CLI due to `$` variable expansion in your Terminal shell.

* LambdaFunctionName=athenaclickhouseconnectorfunction
* DisableSpillEncryption=true [optional]
* SecretNamePrefix=AthenaClickhouse [optional]
* LOG_LEVEL=info [optional]
* **DefaultConnectionString**=clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?user=foo&password=bar&sslmode=none [direct]
* **DefaultConnectionString**=clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?${AthenaClickhouse}&sslmode=none [indirect]
* **SpillBucket**=my-athena-demo 
* **SpillPrefix**=athena-spill
* **SecurityGroupIds**=sg-ab9282d4
* **SubnetIds**=subnet-bc1f0ac6,subnet-db9f40b0 
* **LambdaS3CodeUriBucket**=my-athena-demo
* **LambdaS3CodeUriKey**=code/athena-clickhouse-2024.19.1.jar

**Links**
* https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html
* https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source-lambda.html
* https://docs.aws.amazon.com/athena/latest/ug/connectors-mysql.html  
* https://github.com/awslabs/aws-athena-query-federation/wiki
* https://github.com/awslabs/aws-athena-query-federation/wiki/Deploy-the-Athena-PostgreSQL-Connector-without-using-SAM 
* https://db-engines.com/en/system/ClickHouse


