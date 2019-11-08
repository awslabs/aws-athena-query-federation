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
# Run this script from the athena-federation-sdk-tools director:
# 1. Builds the maven project, if needed.
# 2. Simulates an Athena query running against your connector that is deployed as a Lambda function.
#
# NOTE: That this test may cause a full table scan against your data source. If prompted to provide a
# query predicate, doing so will avoid a full table scan. You can also opt to stop the simulated query
# after the 'planning phase' so that it does not simulate process any splits.
#
EOF

while true; do
    read -p "Do you wish to proceed? (yes or no) " yn
    case $yn in
        [Yy]* ) echo "Proceeding..."; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$#" -lt 1 ]; then
    echo "\n\nERROR: Script requires at least 1 argument \n"
    echo "\n1. Lambda function name.\n"
    echo "\n2. (Optional) catalog id for the query (often the same as the function name) \n"
    echo "\n3. (Optional) schema name to query. \n"
    echo "\n4. (Optional) table name to query. \n"
    echo "\n5. (Optional) record-function if you are using separate Lambda functions for metadata and data, if not you can set to same as arg 1. \n"
    echo "\n\n USAGE from the athena-federation-sdk-tools directory: ../tools/validate_connector.sh cloudwatch \n"
    exit;
fi

if test -d "athena-federation-sdk-tools"; then
  echo "We are not in the correct directory, switching to ./athena-federation-sdk-tools"
  cd athena-federation-sdk-tools
elif test -d "../athena-federation-sdk-tools"; then
  echo "We are not in the correct directory, switching to ../athena-federation-sdk-tools"
  cd ../athena-federation-sdk-tools
elif test -f "src/main/java/com/amazonaws/athena/connector/sanity/ConnectorSanityCheck.java"; then
    echo "Athena Federation SDK Tools classes found, we appear to be in the correct directory."
else
  echo "We are not in the correct directory...and I'm not sure where athena-federation-sdk-tools can be found."
  exit;
fi

if test -f "target/athena-federation-sdk-tools-1.0-withdep.jar"; then
    echo "Looks like the athena-federation-sdk-tools is already built, skipping compilation."
else
    mvn clean install
fi

java -cp target/athena-federation-sdk-tools-1.0-withdep.jar com.amazonaws.athena.connector.sanity.ConnectorSanityCheck $1 $2 $3 $3 $5
