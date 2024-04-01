import os
import boto3
import pandas as pd
from pprint import pprint
from fastparquet import ParquetFile
import snappy
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# from pyathena import connect

def write_to_file(file_path, data):
    """Write data to a file."""
    try:
        with open(file_path, 'w') as file:
            file.write(data)
        print("Data written to file successfully.")
    except Exception as e:
        print(f"Error writing to file: {str(e)}")


s3 = boto3.resource('s3')  # Create the connection to your bucket

bucket = 'aws-public-blockchain'
prefix = 'v1.0/btc'

bucket = s3.Bucket(f"{bucket}")

files = []

for obj in bucket.objects.all():
    key = obj.key
    # print(key)
    if prefix in key and "2024-03" in key and "transactions" in key:
        print(key)
        body = obj.get()['Body'].read()
        file_name = key.split("/")[-1]
    
        dir = f"data{key[len(prefix):-len(file_name)]}"
        print(dir)
        os.makedirs(dir, exist_ok=True)
        buffer = pa.BufferReader(body)
        # Create a PyArrow table from the bytes data
        table = pq.read_table(buffer)
        # Write the PyArrow table to the .snappy.parquet file
        pq.write_table(table, f"{dir}/{file_name}", compression='snappy')