import boto3
import os

s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id='AKIAZ2OXIU5AG7OVWE7I',
    aws_secret_access_key='=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'
)
content="String content to write to a new S3 file"
s3.Object('mercy-locust-covid19-in-dev', 'newfile.txt').put(Body=content)
