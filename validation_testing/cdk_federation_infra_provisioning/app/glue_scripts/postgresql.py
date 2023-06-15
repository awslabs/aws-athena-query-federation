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

type_mapping = {
    'long':'long',
    'int':'int',
    'decimal':'double',
    'string':'varchar',
    'date':'date'
}

def get_postgres_type(glue_type):
    for k,v in type_mapping.items():
        if glue_type.startswith(k): # for things like decimal(x, y)
            return type_mapping[k]
    return glue_type # try to use the same type if no mapping exists

def process_single_table(table_name):
    s3_node = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        format="parquet",
        connection_options = {'paths': [f'{args["s3_full_prefix"]}/{table_name}']}
        )

    apply_mapping = []
    for column in s3_node.toDF().dtypes:
        column_name, glue_type = column[0], column[1]
        apply_mapping.append((column_name, glue_type, column_name, get_postgres_type(glue_type)))


    apply_node = ApplyMapping.apply(
        frame=s3_node,
        mappings=apply_mapping,
        transformation_ctx="apply_mapping_node",
    )

    glueContext.write_dynamic_frame_from_options(
    frame=apply_node,
    connection_type='postgresql',
    connection_options={
        'url': args['db_url'],
        'dbtable': table_name,
        'user': args['username'],
        'password': args['password']
    })

process_single_table(args["tpcds_table_name"])

job.commit()
