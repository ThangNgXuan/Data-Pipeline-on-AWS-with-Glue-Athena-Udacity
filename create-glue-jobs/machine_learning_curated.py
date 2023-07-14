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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://springwin/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node acccelerometer_trusted
acccelerometer_trusted_node1689349568041 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://springwin/accelerometer_trusted/"],
            "recurse": True,
        },
        transformation_ctx="acccelerometer_trusted_node1689349568041",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1689349638537 = ApplyMapping.apply(
    frame=acccelerometer_trusted_node1689349568041,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("z", "double", "z", "double"),
        ("birthDay", "string", "birthDay", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("user", "string", "user", "string"),
        ("y", "double", "y", "double"),
        ("x", "double", "x", "double"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1689349638537",
)

# Script generated for node Join
Join_node1689349573937 = Join.apply(
    frame1=step_trainer_trusted_node1,
    frame2=RenamedkeysforJoin_node1689349638537,
    keys1=["sensorReadingTime", "serialNumber"],
    keys2=["timeStamp", "right_serialNumber"],
    transformation_ctx="Join_node1689349573937",
)

# Script generated for node Drop Fields
DropFields_node1689349577097 = DropFields.apply(
    frame=Join_node1689349573937,
    paths=[
        "right_serialNumber",
        "birthDay",
        "timeStamp",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1689349577097",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1689349577097,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://springwin/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
