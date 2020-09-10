import boto3
from pathlib import Path
import logging
import json
import time


region = 'us-east-1'
imageid = 'ami-02354e95b39ca8dec'
securitygroup = 'launch-wizard-3'
TAG_SPEC = [{"ResourceType":"instance", "Tags": [ {"Key": "Name", "Value": "MercyCorps"}, {"Key": "Environment", "Value": "Test"}]}]
instancetype = 't2.micro'
keyname = 'mercy3'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ec2 = boto3.resource('ec2')
codepipeline = boto3.client('codepipeline')
user_data = """#!/bin/bash -xe
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo chmod 666 /var/run/docker.sock
sleep 2m
unzip /home/ec2-user/locustcovid19.zip -d /home/ec2-user/locustcovid19
sudo chown -R ec2-user:ec2-user /home/ec2-user/locustcovid19
sudo su - ec2-user
chmod 700 /home/ec2-user/locustcovid19/*
/home/ec2-user/locustcovid19/install.sh
"""

def lambda_handler(event, context):
    
    try:
        ec2.create_instances(ImageId=imageid, InstanceType=instancetype, MinCount=1, MaxCount=1, KeyName=keyname, SecurityGroups=[securitygroup], TagSpecifications = TAG_SPEC, IamInstanceProfile={'Name': 'ec2-s3'}, UserData=user_data)
        job_id = event['CodePipeline.job']['id']
        logger.info('Launching ec2 instance ...')
        codepipeline.put_job_success_result(jobId=job_id)

    except Exception as e:
        print("ERROR: " + repr(e))
        message=repr(e)
        traceback.print_exc()
        # Tell CodePipeline we failed
        code_pipeline.put_job_failure_result(jobId=job_id, failureDetails={'message': message, 'type': 'JobFailed'})
