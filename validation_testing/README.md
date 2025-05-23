## Validation Testing Readme

This folder contains both a CDK package which developers can freely use to facilitate their own testing as well as a testing workflow for release tests.

To develop here, you should build your docker image first. assuming you are already in this directory, just run `sh build.sh` to build your image. Then run `IMAGE=federation-validation-testing ~/docker_images/env.sh bash` to start your container.

### Current Testable Connectors
- MySQL
- PostgreSQL
- DynamoDB
- Redshift

### CDK Work

The following will build your CDK project and synthesize your stacks. you can make edits to the stacks or add new ones and then just run `npm run build` and `npm run cdk synth` to refresh the stacks.

```
export CDK_SRC_ROOT=$(dirname $(find . -name ATHENA_INFRA_SPINUP_ROOT));
cd $CDK_SRC_ROOT/app;
npm install;
npm run build;
npm run cdk synth;
npm run cdk ls
```

### Running Release Tests

Pre-reqs: Export aws credentials and the following environment variables:
```
export AWS_DEFAULT_REGION=<region>
export RESULTS_LOCATION=<athena query result s3 path>
export DATABASE_PASSWORD=<db password, for stacks that need a password>
export S3_DATA_PATH=<s3 path where your tpcds data lives>
export REPOSITORY_ROOT=<base dir of repository>
export SPILL_BUCKET=<spill bucket name (not full s3 path)>
```

Then, run `python3 main.py` to test all connectors. If you want to change what connectors are tested, modify the array of connectors in `main.py`, but you can only use ones that already have a corresponding CDK stack and glue job ready.

### Local Development

`sh run_release_tests.sh {CONNECTOR_NAME}` was split up to make it more accessible for local development. First, run `sh deploy_infra.sh {CONNECTOR_NAME}` to deploy database, connector, and etl jobs, then run `sh run_glue.sh {CONNECTOR_NAME}` to load test data (tpcds customer and customer_address tables). Then, run `sh run_tests.sh {CONNECTOR_NAME}` repeatedly for development. Run `sh cleanup.sh {CONNECTOR_NAME}` to cleanup resources. To add/remove tests, modify ./scripts/exec_release_test_queries.py 
