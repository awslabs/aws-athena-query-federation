#!/bin/bash

mvn clean install
sam package --template-file athena-example.yaml --output-template-file packaged.yaml --s3-bucket $1
sam publish --template packaged.yaml
