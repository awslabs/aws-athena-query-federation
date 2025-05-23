# Amazon Athena Lambda Clickhouse Connector

This connector enables Amazon Athena to access your ClickHouse databases. 

Official Public documentation has moved [here](https://docs.aws.amazon.com/athena/latest/ug/connectors-athena.html).

This README walks through the SAM CLI installation method (not Serverless Application Repository via AWS Console).

Deploy a Connector without Serverless Application Repository [link](https://github.com/awslabs/aws-athena-query-federation/wiki/Deploy-a-Connector-without-Serverless-Application-Repository) 

SAM CLI will provide interactive experience to perform deployment.


### Example SAM CLI

SAM CLI guided:
```
cd athena-clickhouse
sam deploy -g --template-file athena-clickhouse.yaml
```

Direct Configuration of Credentials in Connector's connection string without SAM cli guided:
```
cd athena-clickhouse
sam deploy --resolve-s3 --region us-east-1 --template-file athena-clickhouse.yaml --stack-name <stack_name> --capabilities CAPABILITY_NAMED_IAM --parameter-overrides LambdaFunctionName=<function_name> DefaultConnectionString='clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?user=<user>&password=<password>&sslmode=none' SpillBucket=my-athena-demo SecurityGroupIds=sg-1 SubnetIds=subnet-1,subnet-2
```

Indirect Configuration of Credentials in Connector's connection string using AWS Secrets Manager without SAM cli guided:
```
cd athena-clickhouse
sam deploy --resolve-s3 --region us-east-1 --template-file athena-clickhouse.yaml --stack-name <stack_name> --capabilities CAPABILITY_NAMED_IAM --parameter-overrides LambdaFunctionName=<function_name> DefaultConnectionString='clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?${AthenaClickhouse}&sslmode=none' SecretNamePrefix=AthenaClickhouse SpillBucket=my-athena-demo SecurityGroupIds=sg-1 SubnetIds=subnet-1,subnet-2
```
### References

**Parameters** listed below.  You **MUST** change the `DefaultConnectionString`, `SpillBucket`, `SecurityGroupIds` and `SubnetIds`.  

Also, note that there are `DefaultConnectionString` differences depending on whether you directly configure within the URL or indirectly using AWS Secrets Manager.

If you decide to directly configure credentials in the URL, make sure that the URL contains parameters for the `user` and `password`.

If you decide to indirectly configure credentials using AWS Secrets Manager, make sure that the Secret contains parameters for the `username` and `password`.  And make sure that you use single quote when referencing it in the DefaultConnectionString on the Terminal CLI due to `$` variable expansion in your Terminal shell.

* LambdaFunctionName=athenaclickhouseconnectorfunction
* DisableSpillEncryption=true [optional]
* SecretNamePrefix=AthenaClickhouse [optional]
* **DefaultConnectionString**=clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?user=foo&password=bar&sslmode=none [direct]
* **DefaultConnectionString**=clickhouse://jdbc:clickhouse:https://myclickhouseserver.xyzware.io:8443/default?${AthenaClickhouse}&sslmode=none [indirect]
* **SpillBucket**=my-athena-demo 
* **SpillPrefix**=athena-spill [default]
* **SecurityGroupIds**=sg-1
* **SubnetIds**=subnet-1,subnet-2

**Links**
* https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html
* https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source-lambda.html
* https://docs.aws.amazon.com/athena/latest/ug/connectors-mysql.html  
* https://github.com/awslabs/aws-athena-query-federation/wiki
* https://github.com/awslabs/aws-athena-query-federation/wiki/Deploy-the-Athena-PostgreSQL-Connector-without-using-SAM 
* https://db-engines.com/en/system/ClickHouse


