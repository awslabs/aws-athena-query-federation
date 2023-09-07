# Performs the following steps:
#   - Build and deploy CDK stack for a single connector we are testing against
#   - Once the stack finish deploying, start and wait for each glue job to finish
#   - Once those are done, invoke Athena queries against our test data
#   - Once those are done, tear down CDK stack.

CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# upload connector jar to s3 and update yaml to s3 uri, redirect to /dev/null to not log the s3 path
aws s3 cp $REPOSITORY_ROOT/athena-$CONNECTOR_NAME/target/athena-$CONNECTOR_NAME-2022.47.1.jar $S3_JARS_BUCKET > /dev/null
sed -i "s#CodeUri: \"./target/athena-$CONNECTOR_NAME-2022.47.1.jar\"#CodeUri: \"$S3_JARS_BUCKET/athena-$CONNECTOR_NAME-2022.47.1.jar\"#" $REPOSITORY_ROOT/athena-$CONNECTOR_NAME/athena-$CONNECTOR_NAME.yaml

# go to cdk dir, build/synth/deploy
cd $(dirname $(find . -name ATHENA_INFRA_SPINUP_ROOT))/app;

cat <<EOF > .env
DATABASE_PASSWORD=$DATABASE_PASSWORD
S3_DATA_PATH=$S3_PATH
REPOSITORY_ROOT=$REPOSITORY_ROOT
EOF

npm install;
npm run build;
npm run cdk synth;
npm run cdk deploy ${CONNECTOR_NAME}CdkStack > /dev/null;

echo "FINISHED DEPLOYING INFRA FOR ${CONNECTOR_NAME}."

# cd back to validation root
cd $VALIDATION_TESTING_ROOT

# now we run the glue jobs that the CDK stack created
# If there is any output to glue_job_synchronous_execution.py, we will exit this script with a failure code.
# The 2>&1 lets us pipe both stdout and stderr to grep, as opposed to just the stdout. https://stackoverflow.com/questions/818255/what-does-21-mean
echo "Starting glue jobs..."
aws glue list-jobs --max-results 100 \
| jq ".JobNames[] | select(startswith(\"${CONNECTOR_NAME}gluejob\"))" \
| xargs -I{} python3 scripts/glue_job_synchronous_execution.py {} 2>&1 \
| grep -q '.' && exit 1

echo "FINISHED RUNNING GLUE JOBS FOR ${CONNECTOR_NAME}."

# if we are here, it means the above succeeded and we can continue by running our validation tests.

CONNECTOR_LAMBDA_ARN=$(aws lambda get-function --function-name $CONNECTOR_NAME-cdk-deployed | jq ".Configuration.FunctionArn" | tr -d '"') # trim quotes from the json string output
python3 scripts/exec_release_test_queries.py $CONNECTOR_NAME $RESULTS_LOCATION $CONNECTOR_LAMBDA_ARN
RELEASE_TESTS_EXIT_CODE=$?
echo "FINISHED RUNNING TESTS FOR ${CONNECTOR_NAME}, exit code was $RELEASE_TESTS_EXIT_CODE."

# Glue attaches ENIs to the VPC subnet, if there is one, in order to connect to the VPC, and then will issue a delete of them later, but if they aren't finished deleting by the time we invoke cdk destroy,
# CFN will not know about the resources and not be able to delete the stack. So, we proactively look for and delete ENIs attached to glue ourselves first
aws ec2 describe-subnets --filters "Name=tag:Name,Values=${CONNECTOR_NAME}CdkStack*" | jq ".Subnets[].SubnetId" | tr -d '"' \
| xargs -n 1 -I {} aws ec2 describe-network-interfaces --filters "Name=subnet-id,Values={}" | jq ".NetworkInterfaces[] | select(.Description | startswith(\"Attached to Glue\")) | .NetworkInterfaceId" | tr -d '"' \
| xargs -I {} aws ec2 delete-network-interface --network-interface-id {}

# once that is done, we can delete our CDK stack.
cd $(dirname $(find . -name ATHENA_INFRA_SPINUP_ROOT))/app;
# cannot use --force because npm is stripping the flags, so pipe yes through
yes | npm run cdk destroy ${CONNECTOR_NAME}CdkStack;

echo "FINISHED CLEANING UP RESOURCES FOR ${CONNECTOR_NAME}."


echo "FINISHED RELEASE TESTS FOR ${CONNECTOR_NAME}."

exit $RELEASE_TESTS_EXIT_CODE
