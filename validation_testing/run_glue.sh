CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# cd back to validation root
cd $VALIDATION_TESTING_ROOT

# now we run the glue jobs that the CDK stack created
# If there is any output to glue_job_synchronous_execution.py, we will exit this script with a failure code.
# The 2>&1 lets us pipe both stdout and stderr to grep, as opposed to just the stdout. https://stackoverflow.com/questions/818255/what-does-21-mean
echo "Starting glue jobs..."
aws glue list-jobs --max-results 100 \
| jq ".JobNames[] | select(startswith(\"${CONNECTOR_NAME}gluejob\"))" \
| xargs -I{} python3 scripts/glue_job_synchronous_execution.py {} 2>&1 \
| grep -q '.' && exit 1

echo "FINISHED RUNNING GLUE JOBS FOR ${CONNECTOR_NAME}."