CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# go to cdk dir, build/synth/deploy
cd $(dirname $(find . -name ATHENA_INFRA_SPINUP_ROOT))/app;

cat <<EOF > .env
DATABASE_PASSWORD=$DATABASE_PASSWORD
S3_DATA_PATH=$S3_PATH
REPOSITORY_ROOT=$REPOSITORY_ROOT
EOF

# cd back to validation root
cd $VALIDATION_TESTING_ROOT

### There are few objects that need to be cleaned up manually before attempting to destroy the stack:
# 1- ENIs (Affects Postgresql Stack mostly)
# 2- Redhisft Spill bucket can't be deleted because its not empty (Redshift)
# 3- Redshift Parameter Group is required to be cleaned up manually as well

#### STEP 1: 
# Glue attaches ENIs to the VPC subnet, if there is one, in order to connect to the VPC, and then will issue a delete of them later, but if they aren't finished deleting by the time we invoke cdk destroy,
# CFN will not know about the resources and not be able to delete the stack. So, we proactively look for and delete ENIs attached to glue ourselves first
echo "STEP 1: CLEANING UP ANY ENIs"
aws ec2 describe-subnets --filters "Name=tag:Name,Values=${CONNECTOR_NAME}CdkStack*" | jq ".Subnets[].SubnetId" | tr -d '"' \
| xargs -n 1 -I {} aws ec2 describe-network-interfaces --filters "Name=subnet-id,Values={}" | jq ".NetworkInterfaces[] | select(.Description | startswith(\"Attached to Glue\")) | .NetworkInterfaceId" | tr -d '"' \
| xargs -I {} aws ec2 delete-network-interface --network-interface-id {}


#### STEP 2: 
# Cleanup any stack specific S3 bucket:

# List and filter buckets
echo "STEP 2: CLEANING UP ANY S3 RESDUE BUCKETS"
buckets=$(aws s3 ls | grep ${CONNECTOR_NAME}cdkstack | awk '{print $3}')

# Loop through each bucket
for bucket in $buckets; do
    echo "Emptying bucket $bucket"
    aws s3 rm s3://$bucket --recursive --quiet

    echo "Deleting bucket $bucket"
    aws s3 rb s3://$bucket
done

#### STEP3:
# Cleanup Redshift Parameter group 
# #todo: this is results in latest changes of redshift construct
#        We need to move back to L2 contruct, once its fixed
echo "STEP 3: CLEANING UP ANY REDSHIFT PARAMETER GROUP"
if [ "${CONNECTOR_NAME}" = "redshift" ]; then
    aws redshift  delete-cluster-parameter-group -- delete-cluster-parameter-group caseinsensitiveparametername
fi

# once that is done, we can delete our CDK stack if it exists.
cd $(dirname $(find . -name ATHENA_INFRA_SPINUP_ROOT))/app;
prefix="${CONNECTOR_NAME}CdkStack"

npm install;
npm run build;
npm run cdk synth;
yes | npm run cdk destroy ${CONNECTOR_NAME}CdkStack;

# Check if the stack is clean up -- Otherwise fail so that On-Call can manually clean up the stack.
stacks=$(aws cloudformation list-stacks --stack-status-filter CREATE_FAILED ROLLBACK_FAILED DELETE_IN_PROGRESS DELETE_FAILED | jq -r --arg prefix "$prefix" '.StackSummaries[] | select(.StackName | startswith($prefix)) | .StackName')
if [ -z "$stacks" ]; then
    echo "FINISHED CLEANING UP RESOURCES FOR ${CONNECTOR_NAME}."
    # cd back to validation root
    cd $VALIDATION_TESTING_ROOT
    exit 0
else
    echo "FINISHED CLEANING UP RESOURCES FOR ${CONNECTOR_NAME}."
    echo "PLEASE CLEAN UP RESOURCES FOR THE STACK MANUALLY."
    exit 1
fi
