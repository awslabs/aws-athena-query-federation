import boto3
import sys
import time

glue = boto3.client('glue')
poll_interval = 10  # seconds
max_polls = 120 # allow 20 minutes (10 * 120 = 1200 seconds = 1200/60 = 20 minutes)


def start_job_and_wait_for_completion(job_name):
    start_job_response = glue.start_job_run(JobName=job_name)
    job_run_id = start_job_response['JobRunId']
    for polls in range(max_polls):
        status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        if status == 'SUCCEEDED':
            break 
        elif status in ['FAILED', 'STOPPED', 'TIMEOUT']:
            print('Job failed')
            break
        elif polls >= max_polls:
            print('Exceeded max polls')
            break
        else:
            time.sleep(poll_interval)

'''
Script will kickoff a glue job and continuously poll the glue clienet every 10 seconds until it either finishes or 20 minutes pass.
If the script prints anything, that means we did not complete successfully. A successful glue job execution will not print anything.
This allows this script to be called from the command line and easily parse if there's output.

Script should be called with `python3 glue_job_synchronous_execution.py <glue_job_name_here>` and assumes you already have exported credentials.
'''
if __name__ == '__main__':
    job_name = sys.argv[1]
    start_job_and_wait_for_completion(job_name)
