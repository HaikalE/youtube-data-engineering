#!/usr/bin/env python3
import boto3
import yaml

# Load configuration to get bucket name
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

bucket_name = config['aws']['s3_bucket_name']
s3_client = boto3.client('s3')

# List all objects in the bucket
print(f"Listing objects in {bucket_name}:")
paginator = s3_client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name)

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            print(f"- {obj['Key']} ({obj['Size']} bytes, last modified: {obj['LastModified']})")

# Count objects by type
raw_count = 0
processed_count = 0

pages = paginator.paginate(Bucket=bucket_name)
for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            if 'raw/' in obj['Key']:
                raw_count += 1
            elif 'processed/' in obj['Key']:
                processed_count += 1

print(f"\nSummary:")
print(f"- Raw data files: {raw_count}")
print(f"- Processed data files: {processed_count}")
print(f"- Total files: {raw_count + processed_count}")