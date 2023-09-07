import boto3
import sys
import time

athena_client = boto3.client('athena')
connectors_supporting_complex_predicates = ['mysql']
connectors_supporting_limit_pushdown = ['dynamodb', 'mysql']

def run_query(query_string, results_location, workgroup_name, catalog, database):
    """
    Returns (query_id, succeeded)
    """
    query_id = start_query_execution(query_string, results_location, workgroup_name, catalog, database)
    finished_state = wait_until_query_finished(query_id)
    succeeded = finished_state == 'SUCCEEDED'
    if not succeeded: # fast fail on query failures or timeouts.
        raise RuntimeError(f'Query {query_id} terminated in non-successful state. Failing release tests.')
    return query_id


def start_query_execution(query_string, results_location, workgroup_name, catalog, database):
    start_query_resp = athena_client.start_query_execution(
        QueryString=query_string,
        ResultConfiguration={
            'OutputLocation': results_location},
        QueryExecutionContext={
            'Catalog': catalog,
            'Database': database,
        },
        WorkGroup=workgroup_name
    )
    return start_query_resp['QueryExecutionId']

def wait_until_query_finished(query_id):
    max_iterations = 100
    sleep_duration = 5
    query_execution, query_execution_status, query_execution_state = '', '', ''
    iterations = 0
    while not query_execution_state == 'FAILED' and not query_execution_state == 'SUCCEEDED' \
            and iterations < max_iterations:
        iterations += 1
        query_execution_response = athena_client.get_query_execution(
            QueryExecutionId=query_id)
        query_execution = query_execution_response['QueryExecution']
        query_execution_status = query_execution['Status']
        query_execution_state = query_execution_status['State']
        time.sleep(sleep_duration)
    if iterations >= max_iterations:
        return 'TIMED OUT'
    if query_execution_state == 'FAILED':
        print(query_execution)
        print(query_execution['Status']['AthenaError'])
    return query_execution_state


'''
Views are created and stored in the awsdatacatalog. So, although we are passing in the catalog/db associated
with the federated catalog, we actually are overriding this for the view creation/selection/deletion itself.
'''
def test_ddl(connector_name, results_location, workgroup_name, catalog, database):
    glue_db = 'default'
    create_view_query = f'''
        CREATE VIEW default.cx_view_{connector_name} AS
            SELECT c.c_customer_sk, c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_year, c.c_email_address, ca.ca_address_sk, ca_street_number, ca_street_name
            FROM {catalog}.{database}.customer c JOIN {catalog}.{database}.customer_address ca
            ON c.c_current_addr_sk = ca.ca_address_sk
            WHERE c.c_birth_year = 1989; 
    '''
    select_from_view_query = f'SELECT * FROM cx_view_{connector_name} LIMIT 100;'
    drop_view_query = f'DROP VIEW cx_view_{connector_name};'

    # view queries
    run_query(create_view_query, results_location, workgroup_name, catalog, database)
    run_query(select_from_view_query, results_location, workgroup_name, 'awsdatacatalog', glue_db)
    run_query(drop_view_query, results_location, workgroup_name, 'awsdatacatalog', glue_db)
    print('[INFO] - Successfully ran view queries.')

    # other ddl queries
    run_query('show tables;', results_location, workgroup_name, catalog, database)
    run_query('describe customer;', results_location, workgroup_name, catalog, database)
    print('[INFO] - Successfully ran misc DDL queries.')

def test_dml(connector_name, results_location, workgroup_name, catalog, database):
   # 100k rows is around 15mb in mysql, will ensure we test s3 spill as well
    select_star_qid = run_query('SELECT * FROM customer LIMIT 100000', results_location, workgroup_name, catalog, database) 
    select_star_data_scanned = get_data_scanned(select_star_qid)
    print(f'[INFO] - select * query ran successfully, scanned {select_star_data_scanned} bytes')
    
    # ensure projection pushdown occurs and has less data scanned than select *
    projection_pushdown_qid = run_query('SELECT c_customer_sk FROM customer LIMIT 100000', results_location, workgroup_name, catalog, database) 
    projection_pushdown_data_scanned = get_data_scanned(projection_pushdown_qid)
    print(f'[INFO] - projection pushdown query ran successfully, scanned {projection_pushdown_data_scanned} bytes')
    assert_condition(projection_pushdown_data_scanned < select_star_data_scanned, f'FAILURE CONDITION - projection pushdown scans at least as much data as select *. Query ids: {select_star_qid}, {projection_pushdown_qid}')

    # ensure simple predicate pushdown occurs and has less data scanned than select *
    predicate_pushdown_qid = run_query('SELECT * FROM customer WHERE c_customer_sk < 100 LIMIT 100000', results_location, workgroup_name, catalog, database)
    predicate_pushdown_data_scanned = get_data_scanned(predicate_pushdown_qid)
    print(f'[INFO] - predicate pushdown query ran successfully, scanned {predicate_pushdown_data_scanned} bytes')
    assert_condition(predicate_pushdown_data_scanned < select_star_data_scanned, f'FAILURE CONDITION - predicate pushdown scans at least as much data as select *. Query ids: {select_star_qid}, {predicate_pushdown_qid}')

    if connector_name in connectors_supporting_complex_predicates:
        # ensure complex predicate pushdown occurs and has less data scanned than select * and simple predicate pushdown
        complex_predicate_pushdown_qid = run_query('select * from customer where c_customer_sk < 100 AND ((c_current_hdemo_sk + c_current_cdemo_sk) % 2 = 0) limit 100000', results_location, workgroup_name, catalog, database)
        complex_predicate_pushdown_data_scanned = get_data_scanned(complex_predicate_pushdown_qid)
        print(f'[INFO] - complex predicate pushdown query ran successfully, scanned {complex_predicate_pushdown_data_scanned} bytes')
        assert_condition(complex_predicate_pushdown_data_scanned < select_star_data_scanned, f'FAILURE CONDITION - complex predicate pushdown scans at least as much data as select *. Query ids: {select_star_qid}, {complex_predicate_pushdown_qid}')
        assert_condition(complex_predicate_pushdown_data_scanned < predicate_pushdown_data_scanned, f'FAILURE CONDITION - expected complex expression to reduce data scanned compared to simple predicate but it did not. Query ids: {predicate_pushdown_qid}, {complex_predicate_pushdown_qid}')
    else:
        print(f'[INFO] - skipping complex predicate tests for {connector_name}, not supported.')

    if connector_name in connectors_supporting_limit_pushdown:
        # ensure limit pushdown occurs and has less data scanned than select *
        limit_pushdown_qid = run_query('select * from customer limit 1;', results_location, workgroup_name, catalog, database)
        limit_pushdown_data_scanned = get_data_scanned(limit_pushdown_qid)
        print(f'[INFO] - limit pushdown query ran successfully, scanned {limit_pushdown_data_scanned} bytes')
        assert_condition(limit_pushdown_data_scanned < select_star_data_scanned, f'FAILURE CONDITION - limit pushdown scans at least as much data as select *. Query ids: {select_star_qid}, {limit_pushdown_qid}')
    else:
        print(f'[INFO] - skipping limit pushdown tests for {connector_name}, not supported.')

def assert_condition(success_condition, failure_message):
    if not success_condition:
        raise RuntimeError(failure_message)

def get_data_scanned(query_id):
    gqe_response = athena_client.get_query_execution(QueryExecutionId=query_id)
    return gqe_response['QueryExecution']['Statistics']['DataScannedInBytes']

def create_data_source(catalog_name, lambda_arn):
    athena_client.create_data_catalog(Name=catalog_name, Type='LAMBDA', Parameters={'function': lambda_arn})
    print(f'[INFO] - created athena data catalog {catalog_name}')

def delete_data_source(catalog_name):
    athena_client.delete_data_catalog(Name=catalog_name)
    print(f'[INFO] - deleted athena data catalog {catalog_name}')

def run_queries(catalog, connector_name, results_location, lambda_arn):
    '''
    run all our release tests here. we create data catalogs for each connector and delete them after execution.
    this makes the actual queries themselves not need any modifications to append the right JIT lambda catalog name or db name.
    '''

    # db names differ for the datasource, maintain mapping
    db_mapping = {'dynamodb': 'default',
                  'mysql': 'test'
                 }
    workgroup_name = 'primary'
    database = db_mapping[connector_name]
    
    test_dml(connector_name, results_location, workgroup_name, catalog, database),
    test_ddl(connector_name, results_location, workgroup_name, catalog, database),


if __name__ == '__main__':
    connector_name = sys.argv[1]
    results_location = sys.argv[2]
    lambda_arn = sys.argv[3]
    catalog = f'{connector_name}_release_tests_catalog'
    # if any tests fail, they will raise an error which we will not catch so the process returns a non-zero exit code to the calling process.
    # but we always want to clean up our catalog to avoid leaking resources.
    try:
        create_data_source(catalog, lambda_arn)
        run_queries(catalog, connector_name, results_location, lambda_arn)
    finally:
        delete_data_source(catalog)

