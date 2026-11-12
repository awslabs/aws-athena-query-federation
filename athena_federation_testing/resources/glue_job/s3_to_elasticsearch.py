import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', "BUCKET_NAME", "SCALE", "TABLE_NAME", "ES_CONNECTION_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


transformation_ctx = f'os_connector_{args["TABLE_NAME"]}'
# Script generated for node Amazon S3
AmazonS3_node1755281771114 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths":[f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{args["TABLE_NAME"]}']
    },
    transformation_ctx=transformation_ctx)

AmazonS3_node1755281771114.printSchema()

#ES/OS doesn't support decimal, hence convert the type from decimal to double
mappings = []
for field in AmazonS3_node1755281771114.schema():
    name = field.name
    typ = field.dataType.typeName()
    mappings.append((name, typ, name, "double" if typ == "decimal" else typ))  # source and target same

# Script generated for node Change Schema
ChangeSchema_node1755287052222 = ApplyMapping.apply(frame=AmazonS3_node1755281771114, mappings=mappings,
                                                    transformation_ctx=transformation_ctx)
# Script generated for node Amazon OpenSearch Service
glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1755287052222,
    connection_type="opensearch",
    connection_options={"opensearch.resource": f'tpcds{args["SCALE"]}_{args["TABLE_NAME"]}', "connectionName": args["ES_CONNECTION_NAME"]},
    transformation_ctx=transformation_ctx)

job.commit()
