import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1")

# Script generated for node SQL Query
SqlQuery757 = '''
select distinct customer_trusted.*
from accelerometer_landing
inner join customer_trusted
on accelerometer_landing.user=customer_trusted.email;

'''
SQLQuery_node1 = sparkSqlQuery(glueContext, query = SqlQuery757, mapping = {"customer_trusted":CustomerTrusted_node1, "accelerometer_landing":Accelerometerlanding_node1}, transformation_ctx = "SQLQuery_node1")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1 = glueContext.getSink(path="s3://stedi-supraja/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1")
CustomerCurated_node1.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1.setFormat("json")
CustomerCurated_node1.writeFrame(SQLQuery_node1)
job.commit()
