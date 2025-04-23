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

# Script generated for node Step_trainer Landing
Step_trainerLanding_node1745328295463 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="Step_trainerLanding_node1745328295463")

# Script generated for node Customer Curated
CustomerCurated_node1745328298061 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1745328298061")

# Script generated for node SQL Query
SqlQuery848 = '''
select step_trainer_landing.*
from step_trainer_landing
inner join customer_curated
on step_trainer_landing.serialNumber=customer_curated.serialNumber;

'''
SQLQuery_node1745328368864 = sparkSqlQuery(glueContext, query = SqlQuery848, mapping = {"step_trainer_landing":Step_trainerLanding_node1745328295463, "customer_curated":CustomerCurated_node1745328298061}, transformation_ctx = "SQLQuery_node1745328368864")

# Script generated for node Step_trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745328368864, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745328227778", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step_trainerTrusted_node1745328522910 = glueContext.getSink(path="s3://stedi-lake-house-my-1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_trainerTrusted_node1745328522910")
Step_trainerTrusted_node1745328522910.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
Step_trainerTrusted_node1745328522910.setFormat("json")
Step_trainerTrusted_node1745328522910.writeFrame(SQLQuery_node1745328368864)
job.commit()