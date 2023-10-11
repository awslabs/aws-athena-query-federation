
CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# upload connector jar to s3 and update yaml to s3 uri, redirect to /dev/null to not log the s3 path
aws s3 cp $REPOSITORY_ROOT/athena-$CONNECTOR_NAME/target/athena-$CONNECTOR_NAME-2022.47.1.jar "$S3_JARS_BUCKET/" > /dev/null
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

sed -i "s#CodeUri: \"$S3_JARS_BUCKET/athena-$CONNECTOR_NAME-2022.47.1.jar\"#CodeUri: \"./target/athena-$CONNECTOR_NAME-2022.47.1.jar\"#" $REPOSITORY_ROOT/athena-$CONNECTOR_NAME/athena-$CONNECTOR_NAME.yaml

echo "FINISHED DEPLOYING INFRA FOR ${CONNECTOR_NAME}."
