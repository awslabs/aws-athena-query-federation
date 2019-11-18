# Amazon Athena Query Federation SDK Tools

This module contains a collection of tools that are helpful in developing and testing Athena Query Federation components such as connectors. A detailed list
of the tools that can be found in this module can be found below.

### Connector Validator
A runnable class which emulates the calls that Athena will make to your Lambda function as part of executing a
    select * from <database>.<table> where <optional constraint>. The goal of this tool is to help you troubleshoot connectors by giving you visibility of what 'Athena' would
    see. You can run this tool by using the helper script in the tools directory of this repository. Usage details can be found below.
    
```bash
usage: ./validate_connector.sh --lambda-func lambda_func [--record-func record_func] [--catalog catalog] [--schema schema [--table table
                               [--constraints constraints]]] [--planning-only] [--help]
 -c,--constraints <arg>   A comma-separated list of field/value pair constraints to be applied when reading metadata and records from the Lambda
                          function to be validated
 -f,--lambda-func <arg>   The name of the Lambda function to be validated. Uses your configured default AWS region.
 -h,--help                Prints usage information.
 -p,--planning-only       If this option is set, then the validator will not attempt to read any records after calling GetSplits.
 -r,--record-func <arg>   The name of the Lambda function to be used to read data records. If not provided, this defaults to the value provided for
                          lambda-func. Uses your configured default AWS region.
 -s,--schema <arg>        The schema name to be used when validating the Lambda function. If not provided, a random existing schema will be chosen.
 -t,--table <arg>         The table name to be used when validating the Lambda function. If not provided, a random existing table will be chosen.
```