---
name: Bug report
about: Create a report to help us improve
title: "[BUG] <descrption of issue> with connector <connector type>"
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Screenshots / Exceptions / Errors**
If applicable, add screenshots, exception stack traces, or error messages to help explain your problem.  Stack traces for exceptions thrown by your Connector can be found in CloudWatch Logs under the 'aws/lambda/<Lambda_function_name>' log group.

**Connector Details (please complete the following information):**
 - Version: [e.g. 2020.04.01 - if you are unsure, look at the tags on the Lambda function. One of the tag should contain the version if you deployed via Serverless Application Repo.]
 - Name [e.g. redis, hbase, jdbc]
 - Athena Query IDs [if applicable]

**Additional context**
Add any other context about the problem here.
