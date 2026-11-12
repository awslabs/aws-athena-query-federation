import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', "BUCKET_NAME", "SCALE", "TABLE_NAME", "BQ_CONNECTION_NAME", "BQ_PROJECT_ID"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



# Script generated for node Amazon S3
s3node = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths":[f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{args["TABLE_NAME"]}']
    },
    transformation_ctx="reading from s3")

GoogleBigQuery_node1755624395311 = glueContext.write_dynamic_frame.from_options(
    frame=s3node,
    connection_type="bigquery",
    connection_options={
        "connectionName": args["BQ_CONNECTION_NAME"],
        "writeMethod": "direct",
        "parentProject": args["BQ_PROJECT_ID"],
        "table": f'tpcds{args["SCALE"]}.{args["TABLE_NAME"]}'
    },
    transformation_ctx="GoogleBigQuery_node1755624395311")

job.commit()
