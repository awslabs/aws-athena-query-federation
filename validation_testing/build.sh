#!/bin/bash
SCRIPT_PATH=$(dirname $(readlink -f $0))

cd $SCRIPT_PATH

docker image build -t federation-validation-testing .

