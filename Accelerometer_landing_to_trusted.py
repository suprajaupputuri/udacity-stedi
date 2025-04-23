import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1")

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1")

# Script generated for node Join
Join_node1 = Join.apply(frame1=AccelerometerLanding_node1, frame2=CustomerTrusted_node1, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1")

# Script generated for node Drop Fields
DropFields_node1 = DropFields.apply(frame=Join_node1, paths=["email", "phone", "customername", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745326395505", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1 = glueContext.getSink(path="s3://stedi-supraja/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1")
AccelerometerTrusted_node1.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1.setFormat("json")
AccelerometerTrusted_node1.writeFrame(DropFields_node1)
job.commit()
