import boto3, time, logging, os
from botocore.exceptions import ClientError
from athena_federation_testing.util.tpcds import tpcds_reader

from athena_federation_testing.infra import common_infra
from athena_federation_testing.util.client import aws_client_factory, glue_job_client
from athena_federation_testing.util import config_helper

glue_client = aws_client_factory.get_aws_client("glue")
s3_client = aws_client_factory.get_aws_client("s3")

glue_connection_name = "athena_integ_tpcds_glue_3_0"
glue_job_name = "athena_integ_tpcds_generator"
glue_job_number_workers = 3
tpcds_data_generation_script = "tpcds_data_generation_to_s3.py"

# TPCDS glue connection template
connection_input = {
    "Name": glue_connection_name,
    "Description": "tpcds_glue 3.0",
    "ConnectionType": "MARKETPLACE",
    "MatchCriteria": [
        "Connection",
        "TPCDS Connector for Glue 3.0"
    ],
    "ConnectionProperties": {
        "CONNECTOR_CLASS_NAME": "tpcds",
        "CONNECTOR_TYPE": "Spark",
        "CONNECTOR_URL": "https://709825985650.dkr.ecr.us-east-1.amazonaws.com/amazon-web-services/glue-tpcds:1.0.0-glue3.0-2"
    }
}
tables = tpcds_reader.get_data_tables()
# Example of full list
# tables = [
#     "call_center",
# 	"catalog_page",
# 	"catalog_returns",
# 	"catalog_sales",
# 	"customer",
# 	"customer_address",
# 	"customer_demographics",
# 	"date_dim",
# 	"dbgen_version",
# 	"household_demographics",
# 	"income_band",
# 	"inventory",
# 	"item",
# 	"promotion",
# 	"reason",
# 	"ship_mode",
# 	"store",
# 	"store_returns",
# 	"store_sales",
# 	"time_dim",
# 	"warehouse",
# 	"web_page",
# 	"web_returns",
# 	"web_sales",
# 	"web_site"
# ]


def _createGlueConnection():
    try:
        response = glue_client.create_connection(ConnectionInput=connection_input)
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            logging.info(f"Connection already exists:{connection_input}")
        else:
            raise


def _create_glue_job(job_name, script_location, role_arn, arguments=None):
    logging.info(f"job not found: {job_name}. creating..")
    try:
        # First create the job
        response = glue_client.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-enable'
            },
            GlueVersion='3.0',
            MaxRetries=0,
            Timeout=2880,  # 48 hours
            NumberOfWorkers=glue_job_number_workers,
            WorkerType="G.4X",
            Connections={
                "Connections":[glue_connection_name]
            }
        )
        logging.info(f"Created job {job_name}")

    except Exception as e:
        logging.error(f"Error creating/starting job: {str(e)}")
        raise e

def _check_if_bucket_has_data():
    counter = 0
    for table in tables:
        response = s3_client.list_objects_v2(Bucket=config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
                                             Prefix=f'data/tpcds_{config_helper.get_tpcds_scale_factor_number()}/{table}',
                                             MaxKeys=1)
        if 'Contents' in response:
            counter +=1
    if counter == len(tables):
        return True
    elif counter == 0:
        return False
    else:
        raise Exception(f"partial data detected, please clear all data first. bucket:{config_helper.get_glue_job_bucket_name(common_infra.get_account_id())}, "
                        f"key:data/tpcds_{config_helper.get_tpcds_scale_factor()}/{table}")

def generate_tpcds_data():
    # check bucket exsits if not create
    common_infra.construct_glue_job_bucket_if_not_exist()

    # check data already exists
    if _check_if_bucket_has_data():
        logging.info("data already exists, skipping data generator")
        return

    # create glue connection
    _createGlueConnection()


    job_arguments = {
        '--additional-python-modules': 'pandas==1.4.0',
        '--conf': 'spark.sql.shuffle.partitions=100',
        '--BUCKET_NAME': config_helper.get_glue_job_bucket_name(common_infra.get_account_id()),
        '--SCALE': config_helper.get_tpcds_scale_factor_number(),
        '--NUM_PARTITIONS':'48', #3 workers with 16cpu
        '--CONNECTION_NAME': glue_connection_name,
        '--TABLE_LIST': ','.join(tables)
    }

    if not glue_job_client.is_glue_job_exists(glue_job_name):
        # upload job script to the bucket
        logging.info("Uploading job script")
        glue_job_client.upload_glue_job_script('tpcds_data_generation_to_s3.py')
        _create_glue_job(glue_job_name,
                              f's3://{config_helper.get_glue_job_bucket_name(common_infra.get_account_id())}/scripts/{tpcds_data_generation_script}',
                              config_helper.get_glue_job_role_arn(),
                              job_arguments)
    # check if job exists if not create and start

    run_id = glue_job_client.start_job(glue_job_name, job_arguments)
    final_status = glue_job_client.monitor_job_run(glue_job_name, run_id)


# Example usage

