#!/bin/bash

# Copyright (C) 2019 Amazon Web Services
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cat << EOF
# Run this script from the directory of the module (e.g. athena-example) that you wish to publish.
# This script performs the following actions:
# 1. Builds the maven project
# 2. Creates a Serverless Application Package using the athena-example.yaml
# 3. Produces a final packaged.yaml which can be used to publish the application to your
#     private Serverless Application Repository or deployed via Cloudformation.
# 4. Uploads the packaged connector code to the S3 bucket you specified.
# 5. Uses sar_bucket_policy.json to grant Serverless Application Repository access to our connector code in s3.
# 6. Published the connector to you private Serverless Application Repository where you can 1-click deploy it.
EOF

while true; do
    read -p "Do you wish to proceed? (yes or no) " yn
    case $yn in
        [Yy]* ) echo "Proceeding..."; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$#" -lt 2 ]; then
    echo "\n\nERROR: Script requires 3 arguments \n"
    echo "\n1. S3_BUCKET used for publishing artifacts to Lambda/Serverless App Repo.\n"
    echo "\n2. The connector module to publish (e.g. athena-exmaple or athena-cloudwatch) \n"
    echo "\n3. The AWS REGION to target (e.g. us-east-1 or us-east-2) \n"
    echo "\n\n USAGE from the module's directory: ../tools/publish.sh my_s3_bucket athena-example \n"
    exit;
fi

if test -f "$2".yaml; then
    echo "SAR yaml found. We appear to be in the right directory."
else
  echo "SAR yaml not found, attempting to switch to module directory."
  cd $2
fi

REGION=$3
if [ -z "$REGION" ]
then
      REGION="us-east-1"
fi

echo "Using AWS Region $REGION"


if ! aws s3api get-bucket-policy --bucket $1 --region $REGION| grep 'Statement' ; then
    echo "No bucket policy is set on $1 , would you like to add a Serverless Application Repository Bucket Policy?"
    while true; do
        read -p "Do you wish to proceed? (yes or no) " yn
        case $yn in
            [Yy]* ) echo "Proceeding..."
                cat > sar_bucket_policy.json <<- EOM
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service":  "serverlessrepo.amazonaws.com"
      },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::$1/*"
    }
  ]
}
EOM
                cat sar_bucket_policy.json
                set -e
                aws s3api put-bucket-policy --bucket $1 --region $REGION --policy  file://sar_bucket_policy.json
                rm sar_bucket_policy.json
                break;;
            [Nn]* ) echo "Skipping bucket policy, not that this may result in failed attempts to publish to Serverless Application Repository"; break;;
            * ) echo "Please answer yes or no.";;
        esac
    done
fi

set -e
mvn clean install -Dpublishing=true

sam package --template-file $2.yaml --output-template-file packaged.yaml --s3-bucket $1 --region $REGION
sam publish --template packaged.yaml --region $REGION

