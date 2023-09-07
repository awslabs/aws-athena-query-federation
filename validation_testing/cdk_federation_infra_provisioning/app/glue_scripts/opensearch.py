import re
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import array, struct, col


args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_full_prefix', 'username', 'password', 'domain_endpoint', 'glue_connection'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# static list of tpcds tables:
tpcds_table_names = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer_address', 'customer_demographics', 'customer', 'date_dim', 'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason', 'ship_mode', 'store_returns', 'store_sales', 'store', 'time_dim', 'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site']

def process_single_table(table_name):
    print(f'{args["s3_full_prefix"]}/{table_name}')
    
    
    tpcds_s3_node = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        format="parquet",
        connection_options = {'paths': [f'{args["s3_full_prefix"]}/{table_name}']}
        )

    decimal_to_double = lambda x: re.sub(r'decimal\(\d+,\d+\)', 'double', x)
    tpcds_s3_node_dataframe = tpcds_s3_node.toDF()
    resolved_types_dataframe = tpcds_s3_node_dataframe.select(*[ col(d[0]).astype(decimal_to_double(d[1])) if 'decimal' in d[1] else col(d[0]) for d in tpcds_s3_node_dataframe.dtypes ])
    nulls_replaced = resolved_types_dataframe.fillna("")

    updated_dynamic_frame = DynamicFrame.fromDF(nulls_replaced, glueContext, "updated_dynamic_frame")
    # updated_dynamic_frame.printSchema()

    glueContext.write_dynamic_frame_from_options(
        frame=updated_dynamic_frame,
        connection_type = "marketplace.spark",
        connection_options = {
            "path": table_name,
            "es.nodes.wan.only": "true",
            "es.nodes":args['domain_endpoint'],
            "connectionName": args["glue_connection"],
            "es.port":"443",
            "es.net.http.auth.user": args["username"],
            "es.net.http.auth.pass": args["password"]
        }
    )
    print("Table: " + table_name + " Upload Completed!")
    
for table in tpcds_table_names:
    process_single_table(table)

job.commit()