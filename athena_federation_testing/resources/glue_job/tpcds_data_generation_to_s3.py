from socket import create_connection
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "SCALE", "NUM_PARTITIONS", "CONNECTION_NAME", "TABLE_LIST"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

tables = [table.strip() for table in args["TABLE_LIST"].split(",")]

# tables = [
#     "call_center",
# 	"catalog_page",
# 	"catalog_returns",
# 	"catalog_sales",
# 	"customer",
# 	"customer_address",
# 	"customer_demographics",
# 	"date_dim",
# 	"dbgen_version",
# 	"household_demographics",
# 	"income_band",
# 	"inventory",
# 	"item",
# 	"promotion",
# 	"reason",
# 	"ship_mode",
# 	"store",
# 	"store_returns",
# 	"store_sales",
# 	"time_dim",
# 	"warehouse",
# 	"web_page",
# 	"web_returns",
# 	"web_sales",
# 	"web_site"
# ]


for table in tables:
  test = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
      "table": table,
      "scale": args["SCALE"],
      "numPartitions": args["NUM_PARTITIONS"],
      "connectionName": args["CONNECTION_NAME"],
    },
    transformation_ctx=f"TPCDSConnectorforGlue_{table}",
  )
  glueContext.write_dynamic_frame.from_options(
      frame=test, connection_type="s3", format="glueparquet",
      connection_options={"path": f's3://{args["BUCKET_NAME"]}/data/tpcds_{args["SCALE"]}/{table}/', "partitionKeys": []},
      format_options={"compression": "snappy"}, transformation_ctx=f'TPCDSConnectorforGlue_{table}')

job.commit()