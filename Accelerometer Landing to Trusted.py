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
AccelerometerLanding_node1745326745641 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1745326745641")

# Script generated for node Customer Trusted
CustomerTrusted_node1745326748955 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745326748955")

# Script generated for node Join
Join_node1745326856033 = Join.apply(frame1=AccelerometerLanding_node1745326745641, frame2=CustomerTrusted_node1745326748955, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745326856033")

# Script generated for node Drop Fields
DropFields_node1745326929497 = DropFields.apply(frame=Join_node1745326856033, paths=["email", "phone", "customername", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1745326929497")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1745326929497, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745326395505", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1745327006721 = glueContext.getSink(path="s3://stedi-lake-house-my-1/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1745327006721")
AccelerometerTrusted_node1745327006721.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1745327006721.setFormat("json")
AccelerometerTrusted_node1745327006721.writeFrame(DropFields_node1745326929497)
job.commit()