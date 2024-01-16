import subprocess
import os
import sys

TESTABLE_CONNECTORS = ['dynamodb', 'mysql', 'postgresql', 'redshift']

def run_single_connector_release_tests(connector_name):
    shell_command = f'sh run_release_tests.sh {connector_name}'
    # check=True means we will throw an Exception if the subprocess exits with a non-zero response code.
    subprocess.run(shell_command, shell=True, check=True)

def run_all_connector_release_tests():
    for connector in TESTABLE_CONNECTORS:
        run_single_connector_release_tests(connector)

def assert_required_env_vars_set():
    required_env_vars = [
        'RESULTS_LOCATION',
        'REPOSITORY_ROOT',
        'DATABASE_PASSWORD',
        'S3_DATA_PATH',
        'S3_JARS_BUCKET',
        'SPILL_BUCKET'
    ]
    if not all([os.environ.get(env_var) for env_var in required_env_vars]):
        raise RuntimeError("not all expected environment variables were set!")

def pre_test_infra_check():
    # Responsible for making sure that all stacks from previous runs are cleaned
    # This is needed as sometimes we have resources that are stuck around after tests
    # That makes the stack deletion impossible, such as ENIs. 
    # This will ensure that everything is cleaned up before bringing up a new stack
    for connector in TESTABLE_CONNECTORS:
      shell_command = f'sh run_cleanup_infra.sh {connector}'
      subprocess.run(shell_command, shell=True, check=True)

if __name__ == '__main__':
    assert_required_env_vars_set()
    pre_test_infra_check()
    run_all_connector_release_tests() 
