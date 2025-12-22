import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'SCALE', 'TABLE_NAME', 'SF_CONNECTION_NAME', 'SF_DATABASE_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1755717146034 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": [f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{args["TABLE_NAME"]}'], "recurse": True}, transformation_ctx="AmazonS3_node1755717146034")

# Script generated for node Snowflake
Snowflake_node1755717152900 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1755717146034, connection_type="snowflake", connection_options={
    "autopushdown": "on",
    "dbtable": f"{args['TABLE_NAME']}_upper",
    "connectionName": args['SF_CONNECTION_NAME'],
    "sfDatabase": args['SF_DATABASE_NAME'],
    "sfSchema": f"\"tpcds{args['SCALE']}\""}, transformation_ctx="Snowflake_node1755717152900")

job.commit()