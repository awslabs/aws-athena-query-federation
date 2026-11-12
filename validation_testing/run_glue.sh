CONNECTOR_NAME=$1
VALIDATION_TESTING_ROOT=$REPOSITORY_ROOT/validation_testing

# cd back to validation root
cd $VALIDATION_TESTING_ROOT

# now we run the glue jobs that the CDK stack created
# If there is any output to glue_job_synchronous_execution.py, we will exit this script with a failure code.
# The 2>&1 lets us pipe both stdout and stderr to grep, as opposed to just the stdout. https://stackoverflow.com/questions/818255/what-does-21-mean
echo "Starting glue jobs..."
# Capture matching job names
JOB_NAMES=$(aws glue list-jobs \
    | jq -r ".JobNames[] | select(startswith(\"${CONNECTOR_NAME}gluejob\"))")

# Fail early if no jobs matched
if [ -z "$JOB_NAMES" ]; then
    echo "ERROR: No Glue jobs found matching: ${CONNECTOR_NAME}gluejob"
    exit 1
fi

echo "Running jobs:"
echo "$JOB_NAMES"

# Run each job
echo "$JOB_NAMES" \
| xargs -I{} python3 scripts/glue_job_synchronous_execution.py {} 2>&1 \
| grep -q '.' && exit 1

echo "FINISHED RUNNING GLUE JOBS FOR ${CONNECTOR_NAME}."