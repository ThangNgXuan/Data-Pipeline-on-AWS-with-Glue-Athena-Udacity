# Build the ETL process with AWS Glue and AWS Athena on AWS

## Project Scope

- Store the raw data in **S3 bucket** as a Landing Zone.
- Create the data catalog applying the schema for each table in the Landing Zone using **Glue Catalog**.
- Create **Glub Jobs** that processes or transforms the data including to filter, exclude null, and join data. Then, move the cleaned data into **S3 bucket** as a Trusted or Curated Zone.
- Use **Athena** in order to query data in the **Glue tables** and implement some of anylytics purposes.

### Getting Started

- Create IAM Role for AWS Glue and S3 access to other services.
- Copy data from external source into S3 Bucket as a Landing Zone.
    ```
    aws s3 cp ./project/starter/accelerometer/ s3://springwin/accelerometer/ --recursive
    ```

### Project instruction

- Create 2 **Glue tables** for customer_landing and accelerometer_landing. Then use **Athena** to test the queries running.
- Create 2 **Glue Jobs** for customer_landing_to_trusted and accelerometer_landing_to_trusted filtering people who agreed to share their data. 
- Create 2 **Glue tables** for customer_trusted and accelerometer_trusted. Then use **Athena** to test the queries running.
- Create 1 **Glue Job** for customer_trusted_curated including only customer email.
- Create 1 **Glue table** for customer_curated. Then use **Athena** to test the queries running.
- Create 1 **Glue Job** for step_trainer_trusted joining customer_curated and step_trainer_landing.
- Create 1 **Glue Job** for machine_learning_curated joining step_trainer_trusted and accelerometer_trusted.

### AWS Architecture
![image](result_image\aws_architecture.gif)