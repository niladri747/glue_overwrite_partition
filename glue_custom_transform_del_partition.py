# Import necessary AWS Glue and Spark libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Custom transformation function to handle partition management and data processing
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Convert DynamicFrame to Spark DataFrame for easier manipulation
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    # Extract target database name, table name, and S3 path from the first row
    tgt_db_name = df.select('tgt_db_name').limit(1).first()[0]
    tgt_tbl_name = df.select('tgt_tbl_name').limit(1).first()[0]
    target_s3_path = df.select('tgt_s3_path').limit(1).first()[0]    
    
    # Get all unique file dates for partition management
    distinct_file_date = df.select('file_date').distinct().collect()
    
    # Initialize AWS Glue client for API operations
    import boto3
    glue_client = boto3.client('glue')
    
    # Check if target database exists in Glue Catalog
    try:
        glue_client.get_database(Name=tgt_db_name)
        print("Database exists. Skipping database creation.")
    except glue_client.exceptions.EntityNotFoundException:
        # If database doesn't exist, return data without partition deletion
        print("Database does not exist. Skipping partition deletion.")
        dyf = DynamicFrame.fromDF(df, glueContext, "filter2")
        return(DynamicFrameCollection({"CustomTransform0": dyf}, glueContext))
    
    # Check if target table exists in the database
    try:
        glue_client.get_table(DatabaseName=tgt_db_name, Name=tgt_tbl_name)
    except glue_client.exceptions.EntityNotFoundException:
        # If table doesn't exist, return data without partition deletion
        print("Table does not exist in database. Skipping partition deletion.")
        dyf = DynamicFrame.fromDF(df, glueContext, "filter2")
        return(DynamicFrameCollection({"CustomTransform0": dyf}, glueContext))
    
    # Create comma-separated string of file dates
    partition_string = ",".join([str(row['file_date']) for row in distinct_file_date])
    # Add quotes around each date for SQL expression
    partition_string_with_quote = ','.join(f"'{e}'" for e in partition_string.split(','))
    
    # Get list of partitions that match the file dates
    partitions_to_delete = glue_client.get_partitions(
        DatabaseName=tgt_db_name,
        TableName=tgt_tbl_name,
        Expression=f"partition_date IN ({partition_string_with_quote})"
    )
    
    # Iterate through each partition that needs to be deleted
    for partition in partitions_to_delete['Partitions']:
        # Delete partition metadata from Glue Catalog
        glue_client.delete_partition(
            DatabaseName=tgt_db_name,
            TableName=tgt_tbl_name,
            PartitionValues=partition['Values']
        )
        # Construct S3 path for the partition
        partition_path = f"{target_s3_path}partition_date={partition['Values'][0]}/"
        print(partition_path)
        # Delete the actual data from S3
        glueContext.purge_s3_path(partition_path, options={"retentionPeriod": 0})

    # Convert final DataFrame back to DynamicFrame and return
    dyf = DynamicFrame.fromDF(df, glueContext, "filter2")
    return(DynamicFrameCollection({"CustomTransform0": dyf}, glueContext))

