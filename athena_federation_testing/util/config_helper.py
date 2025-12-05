import configparser, os
import importlib.resources as resources
from athena_federation_testing.resources import config_files
from athena_federation_testing.resources.infra import cfn_template

# reading config from config_files/config.properties
config_parser = configparser.ConfigParser()
joinPath = resources.files(config_files).joinpath("config.properties")
athena_catalog_name_prefix = 'athena_integ_'
glue_catalog_name_prefix = 'athena_glue_integ_'

with resources.as_file(joinPath) as path:
    config_parser.read(path)

def get_config(group:str, config_name:str):
    return os.getenv(f'{group}_{config_name}'.upper(), config_parser[group][config_name])

def get_athena_catalog_name(connection_type_name:str) -> str:
    return f"{athena_catalog_name_prefix}{connection_type_name}".lower()

def get_glue_catalog_name(connection_type_name:str) -> str:
    return f"{glue_catalog_name_prefix}{connection_type_name}".lower()

def get_region() -> str:
    return get_config('AWS', 'region')

def get_vpc_stack_name() -> str:
    return get_config('VPC', 'vpc_stack_name')

def get_vpc_name() -> str:
    return get_config('VPC', 'vpc_name')

def get_security_group_name() -> str:
    return get_config('VPC', 'sg_name')

def get_subnet1_name() -> str:
    return get_config('VPC', 'subnet1')

def get_s3_spill_bucket_name(account_id:str) -> str:
    return get_config('s3', 'spill_bucket_name').format(get_region(), account_id)

def get_s3_result_bucket_name(account_id:str) -> str:
    return get_config('s3','result_bucket_name').format(get_region(), account_id)

def get_glue_job_bucket_name(account_id:str) -> str:
    return get_config('s3','glue_job_bucket_name').format(get_region(), account_id)

def get_database_stack_name(source_type:str) -> str:
    return get_config(f'{source_type}', 'stack_name')

def get_rds_database_identifier_name(source_type:str) -> str:
    return get_config(f'{source_type}', 'database_identifier')

def get_rds_database_clsuter_name(source_type:str) -> str:
    return get_config(f'{source_type}', 'database_cluster')

def get_lake_formation_admin_role_arn() -> str:
    return get_config('common', 'lf_admin_role_arn')

def get_repo_root() -> str:
    return get_config('common', 'repo_root')

def get_tpcds_scale_factor() -> str:
    return get_config('common', 'tpcds_scale_factor')

def get_tpcds_scale_factor_number():
    return get_tpcds_scale_factor().removeprefix("tpcds")

def get_load_data_model() -> str:
    return get_config('common', 'load_data_mode')

def get_glue_job_role_arn() -> str:
    return get_config('common', 'glue_job_role_arn')

def get_endpoint_url(service:str) -> str:
    if config_parser.has_section(service) and config_parser.has_option(service, f'{service}_endpoint_url'):
        return config_parser[service][f'{service}_endpoint_url'] if config_parser[service][f'{service}_endpoint_url'] else None
    else:
        return None


