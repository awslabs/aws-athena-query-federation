import boto3, logging
from athena_federation_testing.util import config_helper


def get_aws_client(service:str) -> boto3.client:
    if config_helper.get_endpoint_url(service) is None:
        return boto3.client(service, region_name=config_helper.get_region())
    else:
        return boto3.client(service, region_name=config_helper.get_region(), endpoint_url=config_helper.get_endpoint_url(service), verify=False)