import time, logging

from athena_federation_testing.util import config_helper
from athena_federation_testing.util.client import aws_client_factory

rds = aws_client_factory.get_aws_client('rds')
terminal_statuses = {'available','stopped'}


def update_public_access(rds_database_identifier: str, is_enable: bool):
    logging.info(f"Updating Public Access, value:{is_enable}")
    rds.modify_db_instance(
        DBInstanceIdentifier=rds_database_identifier,
        PubliclyAccessible=is_enable,
        ApplyImmediately=True  # Required to apply the change immediately
    )
    _wait_db_instances_available(rds_database_identifier, is_enable)


def _wait_db_instances_available(rds_database_identifier: str, is_enable: bool):
    timeout_seconds = 300
    polling_interval = 10
    start_time = time.time()
    while True:
        response = rds.describe_db_instances(DBInstanceIdentifier=rds_database_identifier)
        db_status = response['DBInstances'][0]['DBInstanceStatus']
        public_access_setting = response['DBInstances'][0]['PubliclyAccessible']

        if db_status in terminal_statuses and public_access_setting == is_enable:
            break

        if time.time() - start_time > timeout_seconds:
            logging.error("Timeout reached: DB instance did not become available within 5 minutes.")
            break

        logging.info(
            f"⏳ Modifying database config still in progress, identifier:{rds_database_identifier} current setting:{public_access_setting}, target setting:{is_enable}, db status:{db_status}... waiting {polling_interval} seconds.")
        time.sleep(polling_interval)

    logging.info(f'✅ Database config updated, identifier:{rds_database_identifier}')

def get_rds_db_host(rds_database_identifier: str):
    response = rds.describe_db_instances(DBInstanceIdentifier=rds_database_identifier)
    return response['DBInstances'][0]['Endpoint']['Address']

def get_db_cluster(database_cluster: str):
    response = rds.describe_db_clusters(DBClusterIdentifier=database_cluster)
    return response['DBClusters'][0]["Endpoint"]