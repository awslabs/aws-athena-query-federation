# Performs the following steps:
#   - Build and deploy CDK stack for a single connector we are testing against
#   - Once the stack finish deploying, start and wait for each glue job to finish
#   - Once those are done, invoke Athena queries against our test data
#   - Once those are done, tear down CDK stack.

CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# ecr repository must be created and an image pushed before the cdk stack is deployed for the lambda to create successfully
# get the AWS account ID from the current roll (for use in ECR repo name)
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

# create the ECR repository
aws ecr create-repository --repository-name athena-federation-repository-$CONNECTOR_NAME --region us-east-1

# push the ECR image for the connector to the ECR repository
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
docker build -t athena-federation-repository-$CONNECTOR_NAME $REPOSITORY_ROOT/athena-$CONNECTOR_NAME
docker tag athena-federation-repository-$CONNECTOR_NAME\:latest $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/athena-federation-repository-$CONNECTOR_NAME\:latest
docker push $ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/athena-federation-repository-$CONNECTOR_NAME\:latest

# update the template to use the correct ImageUri
sed -i "s|292517598671|$ACCOUNT_ID|g" "$REPOSITORY_ROOT/athena-$CONNECTOR_NAME/athena-$CONNECTOR_NAME.yaml"
sed -i "s#\(/athena-federation-repository-$CONNECTOR_NAME:\)[0-9]\{4\}\.[0-9]\{1,2\}\.[0-9]\{1\}#\1latest#" $REPOSITORY_ROOT/athena-$CONNECTOR_NAME/athena-$CONNECTOR_NAME.yaml

echo "FINISHED PUSHING CONNECTOR IMAGE TO ECR REPOSITORY"

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
# Capture matching job names
JOB_NAMES=$(aws glue list-jobs \
    | jq -r ".JobNames[] | select(startswith(\"${CONNECTOR_NAME}gluejob\"))")

# Fail early if no jobs matched
if [ -z "$JOB_NAMES" ]; then
    echo "ERROR: No Glue jobs found matching: ${CONNECTOR_NAME}gluejob"
    exit 1
fi

echo "Running jobs:"
echo "$JOB_NAMES"

# Run each job
echo "$JOB_NAMES" \
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

# delete the ecr repository
aws ecr delete-repository --repository-name athena-federation-repository-$CONNECTOR_NAME --force

echo "FINISHED CLEANING UP RESOURCES FOR ${CONNECTOR_NAME}."


echo "FINISHED RELEASE TESTS FOR ${CONNECTOR_NAME}."

exit $RELEASE_TESTS_EXIT_CODE
