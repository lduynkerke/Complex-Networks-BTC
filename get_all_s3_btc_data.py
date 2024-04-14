import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS credentials from .env file
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

# Create a session using the loaded credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Create an S3 client using the session
s3_client = session.client('s3')

# Bucket name and prefix
bucket_name = 'aws-public-blockchain'
prefix = 'v1.0/btc'

# Function to list all objects in the bucket with pagination
def list_all_objects(bucket_name, prefix):
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        if 'Contents' in page:
            objects.extend(page['Contents'])

    return objects

# List all objects in the bucket with the specified prefix
objects = list_all_objects(bucket_name, prefix)

for obj in objects:
    key = obj['Key']
    if "2024-04" in key and "transactions" in key:  # Filter objects based on conditions
        print("Downloading:", key)
        # Download the object
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        body = response['Body'].read()
        
        file_name = key.split("/")[-1]
        # Extract filename and directory
        directory = f"data{key[len(prefix):-len(file_name)]}"
        
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
        
        # Read Parquet file from buffer
        buffer = pa.BufferReader(body)
        table = pq.read_table(buffer)
        
        # Write Parquet file to disk
        pq.write_table(table, os.path.join(directory, file_name), compression='snappy')
