
import time, logging
from botocore.exceptions import ClientError
from pathlib import Path

from athena_federation_testing.util.client import aws_client_factory


cf = aws_client_factory.get_aws_client("cloudformation")

def is_cfn_exists(stack_name: str) -> bool:
    try:
        response = cf.describe_stacks(StackName=stack_name)
        # If no exception, the stack exists
        return True
    except ClientError as e:
        if 'does not exist' in str(e):
            return False
        else:
            # Re-raise unexpected errors
            raise

def construct_cfn_resource(cfn_resource_path: Path, stack_name: str, parameters:  list[dict[str, str]] = None) -> None:
    logging.info(f'constructing cfn resource, file path:{cfn_resource_path}')
    with open(cfn_resource_path) as template_file:
        template_body = template_file.read()

    if parameters is None:
        cf.create_stack(
            StackName=stack_name,
            TemplateBody=template_body
        )
    else:
        cf.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
        )

    in_progress_statuses = {'CREATE_IN_PROGRESS', 'UPDATE_IN_PROGRESS', 'ROLLBACK_IN_PROGRESS'}
    complete_statuses = {'CREATE_COMPLETE', 'UPDATE_COMPLETE'}
    failed_statuses = {
        'ROLLBACK_COMPLETE',
        'CREATE_FAILED',
        'UPDATE_ROLLBACK_COMPLETE',
        'UPDATE_ROLLBACK_FAILED',
        'DELETE_COMPLETE',
        'DELETE_FAILED'
    }

    while True:
        response = cf.describe_stacks(StackName=stack_name)
        stack_status = response['Stacks'][0]['StackStatus']

        if stack_status in complete_statuses:
            logging.info("✅ Stack creation complete.")
            break
        elif stack_status in failed_statuses:
            raise Exception(f"❌ Stack failed with status: {stack_status}")
        else:
            logging.info(f"⏳ Stack still in progress, stack:{stack_name}, status:{stack_status}... waiting 10 seconds.")
            time.sleep(10)
