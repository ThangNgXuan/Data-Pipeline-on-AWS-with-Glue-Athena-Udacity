import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://springwin/customer_trusted/"], "recurse": True},
    transformation_ctx="customer_trusted_node1",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1689344443203 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://springwin/accelerometer/"], "recurse": True},
    transformation_ctx="accelerometer_landing_node1689344443203",
)

# Script generated for node Join
Join_node1689344483537 = Join.apply(
    frame1=customer_trusted_node1,
    frame2=accelerometer_landing_node1689344443203,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1689344483537",
)

# Script generated for node Drop Fields
DropFields_node1689346649551 = DropFields.apply(
    frame=Join_node1689344483537,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689346649551",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689346649551,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://springwin/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node3",
)

job.commit()
