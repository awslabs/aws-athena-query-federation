import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_full_prefix', 'db_url', 'username', 'password', 'tpcds_table_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_single_table(table_name):
    s3_node = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        format="parquet",
        connection_options = {'paths': [f'{args["s3_full_prefix"]}/{table_name}']}
        )
    
    glueContext.write_dynamic_frame_from_options(
    frame=s3_node,
    connection_type='mysql',
    connection_options={
        'url': args['db_url'],
        'dbtable': table_name,
        'user': args['username'],
        'password': args['password']
    })
    
process_single_table(args["tpcds_table_name"])

job.commit()

