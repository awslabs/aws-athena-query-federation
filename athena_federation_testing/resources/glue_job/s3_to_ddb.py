import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "SCALE", "TABLE_NAME"])
glue_context= GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options = {
        "paths":[f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{args["TABLE_NAME"]}']
    },
    format = "parquet"
)

glue_context.write_dynamic_frame_from_options(
    frame=dyf,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName": f'tpcds{args["SCALE"]}_{args["TABLE_NAME"]}',
        "dynamodb.throughput.write.percent": "1.0"
    }
)

job.commit()