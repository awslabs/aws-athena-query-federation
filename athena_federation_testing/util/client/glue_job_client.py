import boto3, time, logging, os
from botocore.exceptions import ClientError

from athena_federation_testing.infra import common_infra
from athena_federation_testing.util.client import aws_client_factory
from athena_federation_testing.util import config_helper
from pathlib import Path

glue_client = aws_client_factory.get_aws_client("glue")
glue_job_number_workers = 3
glue_job_status_interval = 20


def create_glue_job_connection(connection_input, connection_name):
    try:
        glue_client = aws_client_factory.get_aws_client("glue")
        response = glue_client.create_connection(ConnectionInput=connection_input)
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            logging.info(f"Connection already exists:{connection_name}")
        else:
            raise

def start_job(job_name, arguments=None):
    try:
        if arguments is None:
            arguments = {}

        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )

        job_run_id = response['JobRunId']
        logging.info(f"Started job run {job_run_id} for job {job_name}")
        return job_run_id

    except Exception as e:
        logging.error(f"Error starting job: {str(e)}")
        raise e

# monitor job, this has no time out as it could take a long time
def monitor_job_run(job_name, run_id):
    while True:
        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        status = response['JobRun']['JobRunState']

        logging.info(f"Job {job_name} run {run_id} status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break

        time.sleep(glue_job_status_interval)  # Wait 30 seconds before checking again
    return status

def is_glue_job_exists(job_name):
    try:
        glue_client.get_job(JobName=job_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise

def upload_glue_job_script(script_name):
    test = Path(__file__).parent.parent.parent
    local_file_path = os.path.join(test, 'resources', 'glue_job', script_name)
    # Initialize boto3 S3 client
    s3 = aws_client_factory.get_aws_client('s3')
    # Upload the file
    s3.upload_file(local_file_path, config_helper.get_glue_job_bucket_name(common_infra.get_account_id()), f'scripts/{script_name}')

    logging.info(f"Uploaded {local_file_path} to s3://{config_helper.get_glue_job_bucket_name(common_infra.get_account_id())}/scripts/{script_name}")