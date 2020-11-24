# aws-afq-slackapi

Work in progress...

./publish.sh pablo.athena.federatedqueries athena-slack-api us-east-1


## Prep Dev Environment

Initialize the following environment variables for build process test:

```
    export data_endpoint="https://slack.com/api/admin.analytics.getFile" 
    export region="us-east-1"
    export secret_name="slack_analytics_token_4IJ0t"
    export auth_token="<your_OAuth_token_from_Secrets>"
    export test_date="2020-11-10"
```