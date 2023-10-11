CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing
RELEASE_TESTS_EXIT_CODE=$?

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