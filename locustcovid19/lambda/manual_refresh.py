import json
import boto3
import os
# boto3 S3 initialization
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    
    BUCKET = os.environ['BUCKET']
    BUCKET_DEST = os.environ['BUCKET_DEST']
    MODULE = os.environ['MODULE']   
    
    key = 'module.' + MODULE

    # Copy Source Object
    copy_source_object = {'Bucket': BUCKET, 'Key': key}

    # S3 copy object operation
    s3_client.copy_object(CopySource=copy_source_object, Bucket=BUCKET_DEST, Key='config/moduletag')
