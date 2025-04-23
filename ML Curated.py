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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1")

# Script generated for node Step_trained Trusted
Step_trainedTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="Step_trainedTrusted_node1")

# Script generated for node SQL Query
SqlQuery720 = '''
select step_trained_trusted.sensorreadingtime,
step_trained_trusted.serialnumber,
step_trained_trusted.distancefromobject,
accelerometer_trusted.user,
accelerometer_trusted.x,
accelerometer_trusted.y,
accelerometer_trusted.z
from step_trained_trusted
join accelerometer_trusted
on accelerometer_trusted.timestamp=step_trained_trusted.sensorreadingtime;

'''
SQLQuery_node1 = sparkSqlQuery(glueContext, query = SqlQuery720, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1, "step_trained_trusted":Step_trainedTrusted_node1}, transformation_ctx = "SQLQuery_node1")

# Script generated for node ML Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MLCurated_node1 = glueContext.getSink(path="s3://stedi-supraja/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MLCurated_node1")
MLCurated_node1.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_curated")
MLCurated_node1.setFormat("json")
MLCurated_node1.writeFrame(SQLQuery_node1)
job.commit()
