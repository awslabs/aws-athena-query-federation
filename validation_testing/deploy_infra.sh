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
