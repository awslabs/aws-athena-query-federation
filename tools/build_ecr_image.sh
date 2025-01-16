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
# Run this script from the directory of the module (e.g. athena-example) that you wish to build image and publish to ECR.
# This script performs the following actions:
# 1. Creates the ECR repo in your account for the connector if it does not exist
# 2. Builds the docker image
# 3. Publishes the image to ECR so you can update your connector lambda to use your custom version

NOTE: please make sure your code is previously built with maven before calling this script.
EOF

REGION=$1
if [ -z "$REGION" ]
then
      REGION="us-east-1"
fi

base_dir=$(basename `pwd`)
CONNECTOR_NAME=${base_dir#athena-}
REPO_NAME=athena-federation-repository-${CONNECTOR_NAME}
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
VERSION=$(grep 'SemanticVersion:' "${base_dir}.yaml" | awk '{print $2}')
ECR_BASE_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
ECR_REPO_URL="${ECR_BASE_URL}/${REPO_NAME}"

echo "Using AWS Region $REGION"
echo "Using Connector Name: ${CONNECTOR_NAME}"
echo "Using Repo Name: ${REPO_NAME}"
echo "Using Version: ${VERSION}"
echo "Using AWS Account Id: ${AWS_ACCOUNT_ID}"
echo "Using ECR Repo URL: ${ECR_REPO_URL}"

# Check if the repository exists
EXISTING_REPO=$(aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" 2>/dev/null)

if [ $? -eq 0 ]; then
    echo "Repository '$REPO_NAME' already exists."
else
    echo "Repository '$REPO_NAME' does not exist. Creating..."
    aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION"
    if [ $? -eq 0 ]; then
        echo "Repository '$REPO_NAME' created successfully."
    else
        echo "Failed to create repository '$REPO_NAME'."
        exit 1
    fi
fi

echo "Building docker image..."
docker build --platform linux/amd64 -t ${REPO_NAME} .
if [ $? -ne 0 ]; then
  echo "Error: Docker build failed for ${REPO_NAME}"
  exit 1
fi

echo "Tagging docker image..."
docker tag "${REPO_NAME}:latest" "${ECR_REPO_URL}:${VERSION}"
if [ $? -ne 0 ]; then
  echo "Error: Docker tag failed for ${REPO_NAME}"
  exit 1
fi

aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ECR_BASE_URL}
if [ $? -ne 0 ]; then
  echo "Error: Failure authenticating docker daemon with ECR: ${ECR_BASE_URL}"
  exit 1
fi

echo "Pushing Docker image for ${REPO_NAME}"
docker push "${ECR_REPO_URL}:${VERSION}"
if [ $? -ne 0 ]; then
  echo "Error: Docker push failed for ${REPO_NAME}"
  exit 1
fi
