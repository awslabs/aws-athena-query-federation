## Example Athena Slack Member Analytics Connector

This connector enables Amazon Athena to communicate with your Slack Member Analytics endpoint via SQL. The Slack Member Analytics API allows customers to access their Slack engagement data. The data is available daily for each member in the organization. Customers can then summarize, filter, and aggregate the data using SQL and analyze via their own business intelligence tools (e.g. AWS QuickSight) and terms (department, role etc), compare Slack engagement to other tools, and integrate Slack data into existing dashboards & business processes.
 
**Athena Federated Queries are now enabled as GA in US-East-1 (IAD), US-West-2 (PDX), and US-East-2 (CMH). To use this feature, upgrade your engine version to Athena V2 in your workgroup settings. Check documentation here for more details: https://docs.aws.amazon.com/athena/latest/ug/engine-versions.html.  To enable this feature in other regions, you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

## Usage

### Parameters

1. **AthenaCatalogName** - (Optional) Defaults to 'slackanalytics'. The name you will give to this catalog in Athena. It will also be used as the function name. This name must satisfy the pattern ^[a-z0-9-_]{1,64}$
2. **AWSSecretName** - AWS Secret name where the Slack API OAuth token is stored.
3. **SlackEndpointURL** - (Optional) Defaults to 'https://slack.com/api/admin.analytics.getFile'. Slack Analytics data endpoint.
3. **SpillBucket** - When the data returned by your Lambda function exceeds Lambda’s limits, this is the bucket that the data will be written to for Athena to read the excess from. (e.g. my_bucket)
4. **SpillPrefix** - (Optional) Defaults to sub-folder in your bucket called 'athena-spill'. Used in conjunction with spill_bucket, this is the path within the above bucket that large responses are spilled to. You should configure an S3 lifecycle on this location to delete old spills after X days/Hours.
5. **LambdaTimeout** - (Optional) Defaults to 900. Maximum Lambda invocation runtime in seconds. (min 1 - 900 max)
6. **LambdaMEmory** - (Opitonal) Defaults to 3008. Lambda memory in MB (min 128 - 3008 max).
7. **DisableSpillEncryption** - (Optional) Defaults to false. WARNING: If set to 'true' encryption for spilled data is disabled.

### Schema

Column|Sample Value|Athena Data Type|Details
---|---|---|---
date|2020-09-01|STRING|The date this row of data is representative of
enterprise_id|E2AB3A10F|STRING|Unique ID of the involved Enterprise organization
enterprise_user_id|W1F83A9F9|STRING|The canonical, organization-wide user ID this row concerns
email_address|person@acme.com|STRING|The email address of record for the same user
enterprise_employee_number|273849373|STRING|This field is pulled from data synced via a SCIM API custom attribute
is_guest|false|STRING|User is classified as a guest (not a full workspace member) on the date in the API request
is_billable_seat|true|STRING|User is classified as a billable user (included in the bill ) on the the date in the API request
is_active|true|STRING|User has posted a message or read at least one channel or direct message on the date in the API request
is_active_iOS|true|STRING|User has posted a message or read at least onechannel or direct message on the date in theAPI request via the Slack iOS App on the date inNthe API request
is_active_Android|false|STRING|User has posted a message or read at least one channel or direct message on the date in the API request via the Slack Android App on the date in the API request
is_active_desktop|true|STRING|User has posted a message or read at least one channel or direct message on the date in the API request via the Slack desktop APP on the date in the API request
reactions_added_count|20|INTEGER|Total reactions added to any message type (reply, bot message, channel or dm etc.) on the date in the API request. Removing reactions is not included.<P><P>This metric is not deduplicated by message (if a user add 3 different reactions to a single message, we will report 3 reactions). 
messages_posted_count|40|INTEGER|This is total messages posted (sent) by the user on the date in the API request to all message types (DMs, MPDMs, Private & Public Channels)
channel_messages_posted_count|30|INTEGER|This is messages posted by the user in private channels and public channels on the date in the API request
files_added_count|5|INTEGER|Total files uploaded by the user on the date in the API request

### Deploy your connector

The Slack Member Analytics API is available for Slack Enterprise Grid Customers. You can read more [here](https://slack.com/resources/why-use-slack/slack-enterprise-grid). You will need admin access to your Slack Enterprise Grid to deploy a custom Slack app in your Slack Organization and obtain an OAuth token. 

1. Create a custom Slack App and obtain OAuth Access Token with the user scope of 'admin.analytics:read' following [these instructions](https://api.slack.com/scopes/admin.conversations:write). 

2. Once you obtain an OAuth Token store it in an AWS Secret with secret_key='access_token' and secret_value='your OAuth token'. 

3. Navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. 

Alternatively, you can build and deploy this connector from source following the below steps:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. Initialize the following environment variables with your secret info.

```
    export data_endpoint="https://slack.com/api/admin.analytics.getFile" 
    export region="<AWS_Region>"
    export secret_name="<New_AWS_Secret_Name>"
    export test_date="2020-11-10"
```

3. From the athena-slack-member-analytics dir, run `mvn clean install`.
4. From the athena-slack-member-analytics dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-slack-analytics` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)

### Required Permissions

1. S3 Write Access - In order to successfully handle large queries, the connector requires write access to a location in S3. 
2. SecretsManager Read Access - The connector will need access to the OAuth token stored in AWS secrets.
3. CloudWatch Logs - This is a somewhat implicit permission when deploying a Lambda function but it needs access to cloudwatch logs for storing logs.
4. Athena GetQueryExecution - The connector uses this access to fast-fail when the upstream Athena query has terminated.