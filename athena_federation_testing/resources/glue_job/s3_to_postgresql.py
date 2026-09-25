import sys
# import pymysql
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "SCALE", "TABLE_LIST", "POSTGRESQL_URL", "POSTGRESQL_USER", "POSTGRESQL_PWD"])
glue_context= GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

tables = [table.strip() for table in args["TABLE_LIST"].split(",")]

for table in tables:
    transformation_ctx = f"oracle_connector_{table}"
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options = {
            "paths":[f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{table}']
        },
        format = "parquet",
        transformation_ctx=transformation_ctx
    )

    glue_context.write_dynamic_frame_from_options(
        frame=dyf,
        connection_type="postgresql",
        connection_options={
            "url": args['POSTGRESQL_URL'],
            "dbtable":  f'tpcds{args["SCALE"]}.{table}',
            "user": args['POSTGRESQL_USER'],
            "password": args['POSTGRESQL_PWD']
        },
        transformation_ctx=transformation_ctx
    )
    job.commit()
