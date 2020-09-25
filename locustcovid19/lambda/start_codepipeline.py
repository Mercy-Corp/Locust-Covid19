import json
import boto3
import os

def lambda_handler(event, context):

    codepipeline_client = boto3.client('codepipeline')
    
    codepipeline = os.environ['CODEPIPELINE']
   
    codepipeline_response = codepipeline_client.start_pipeline_execution(name=codepipeline)
