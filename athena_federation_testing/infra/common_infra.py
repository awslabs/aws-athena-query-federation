import logging
from athena_federation_testing.util.client import cfn_client, aws_client_factory
from athena_federation_testing.util import config_helper

import importlib.resources as resources
from athena_federation_testing.resources.infra import cfn_template
from botocore.exceptions import ClientError

s3 = aws_client_factory.get_aws_client("s3")
sts_client = aws_client_factory.get_aws_client('sts')

def _construct_s3_bucket(bucket_name:str) -> None:
    if _is_bucket_exists(bucket_name):
        logging.info(f's3 spill bucket: {bucket_name} already exists')
        return
    logging.info(f's3 spill bucket: {bucket_name}')

    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': config_helper.get_region()})
    logging.info(f"✅ s3 spill bucket creation complete. name: {bucket_name}")

def _is_bucket_exists(bucket_name:str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            return False
        raise

def construct_vpc_stack_if_not_exist() -> None:
    vpc_name = config_helper.get_vpc_stack_name()
    if cfn_client.is_cfn_exists(vpc_name):
        logging.info(f'Stack {vpc_name} already exists')
        return

    yaml_file = resources.files(cfn_template).joinpath("network.yaml")
    with resources.as_file(yaml_file) as path:
        cfn_client.construct_cfn_resource(path, vpc_name)

def destroy_vpc_stack() -> None:
    vpc_stack_name = config_helper.get_vpc_stack_name()
    if not(cfn_client.is_cfn_exists(vpc_stack_name)):
        logging.info(f'Stack {vpc_stack_name} not exists')
        return

    cf = aws_client_factory.get_aws_client("cloudformation")
    response = cf.delete_stack(StackName=vpc_stack_name)
    logging.info(f'Started delete VPC stack: {vpc_stack_name}.')

def construct_s3_spill_bucket_if_not_exist() -> None:
    _construct_s3_bucket(config_helper.get_s3_spill_bucket_name(get_account_id()))

def construct_s3_result_bucket_if_not_exist() -> None:
    _construct_s3_bucket(config_helper.get_s3_result_bucket_name(get_account_id()))

def construct_glue_job_bucket_if_not_exist() -> None:
    _construct_s3_bucket(config_helper.get_glue_job_bucket_name(get_account_id()))

def get_account_id():
    response = sts_client.get_caller_identity()
    return response['Account']