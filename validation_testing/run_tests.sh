CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

CONNECTOR_LAMBDA_ARN=$(aws lambda get-function --function-name $CONNECTOR_NAME-cdk-deployed | jq ".Configuration.FunctionArn" | tr -d '"') # trim quotes from the json string output
python3 scripts/exec_release_test_queries.py $CONNECTOR_NAME $RESULTS_LOCATION $CONNECTOR_LAMBDA_ARN
RELEASE_TESTS_EXIT_CODE=$?
echo "FINISHED RUNNING TESTS FOR ${CONNECTOR_NAME}, exit code was $RELEASE_TESTS_EXIT_CODE."