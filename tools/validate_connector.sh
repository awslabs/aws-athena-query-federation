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
# Run this script from any directory:
# 1. Builds the maven project, if needed.
# 2. Simulates an Athena query running against your connector that is deployed as a Lambda function.
#
# NOTE: That this test may cause a full table scan against your data source. If prompted to provide a
# query predicate, doing so will avoid a full table scan. You can also opt to stop the simulated query
# after the 'planning phase' so that it does not simulate process any splits.
#
# Use the -h or --help args to print usage information.
#
# Use 'yes | tools/validate_connector.sh [args]' to bypass this check. USE CAUTION
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

VERSION=2022.47.1

dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

cd "$dir/../athena-federation-sdk-tools"

if test -f "target/athena-federation-sdk-tools-${VERSION}.jar"; then
    echo "athena-federation-sdk-tools is already built, skipping compilation."
else
    mvn clean install
fi

java -cp target/athena-federation-sdk-tools-${VERSION}-withdep.jar com.amazonaws.athena.connector.validation.ConnectorValidator $@