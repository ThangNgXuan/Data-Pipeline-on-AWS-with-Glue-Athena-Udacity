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

# Script generated for node customer_curated
customer_curated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://springwin/customer_curated/"], "recurse": True},
    transformation_ctx="customer_curated_node1",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1689348994706 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://springwin/step_trainer/"], "recurse": True},
    transformation_ctx="step_trainer_landing_node1689348994706",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1689349041396 = ApplyMapping.apply(
    frame=customer_curated_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("email", "string", "email", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1689349041396",
)

# Script generated for node Join
Join_node1689348995833 = Join.apply(
    frame1=step_trainer_landing_node1689348994706,
    frame2=RenamedkeysforJoin_node1689349041396,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1689348995833",
)

# Script generated for node Drop Fields
DropFields_node1689348999865 = DropFields.apply(
    frame=Join_node1689348995833,
    paths=["right_serialNumber", "email"],
    transformation_ctx="DropFields_node1689348999865",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689348999865,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://springwin/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
