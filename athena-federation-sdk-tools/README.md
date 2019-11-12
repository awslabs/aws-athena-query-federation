# Amazon Athena Query Federation SDK Tools

This module contains a collection of tools that are helpful in developing and testing Athena Query Federation components such as connectors. A detailed list
of the tools that can be found in this module can be found below.

* **Connector Validator** - A runnable which emulates the calls that Athena will make to your Lambda function as part of executing a 
    select * from <database>.<table> where <optional constraint>. The goal of this tool is to help your troubleshoot connectors by giving you visibility of what 'Athena' would
    see. You can run this tool by using the helper script in the tools directory ../tools/validate_connector.sh lambda_func_name