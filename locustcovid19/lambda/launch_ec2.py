import os
import boto3
from pathlib import Path
import logging
import json
import time

# Retrieves environment variables 

region = os.environ['REGION']
imageid = os.environ['IMAGEID']
securitygroup = os.environ['SECURITYGROUP']
instancetype = os.environ['INSTANCETYPE']
keyname = os.environ['KEYNAME']
environment = os.environ['ENVIRONMENT']
instancename = os.environ['INSTANCENAME']

TAG_SPEC = [{"ResourceType":"instance", "Tags": [ {"Key": "Name", "Value": instancename}, {"Key": "Environment", "Value": environment}]}]

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
sleep 1m
(unzip /home/ec2-user/locustcovid19.zip -d /home/ec2-user/locustcovid19 &&
sudo chown -R ec2-user:ec2-user /home/ec2-user/locustcovid19 &&
sudo su - ec2-user &&
cat /home/ec2-user/moduletag >> /home/ec2-user/locustcovid19/locustcovid19/config/application.yaml && 
chmod 700 /home/ec2-user/locustcovid19/* &&
/home/ec2-user/locustcovid19/install.sh) ||
if [ $? -ne 0 ]; then
   touch /home/ec2-user/moduletag
   aws s3 cp /home/ec2-user/moduletag s3://mercy-locust-covid19-config/failed/moduletag
fi
sleep 3m
sudo shutdown -P now
"""

def lambda_handler(event, context):
    
    try:
        ec2.create_instances(ImageId=imageid, InstanceType=instancetype, MinCount=1, MaxCount=1, KeyName=keyname, SecurityGroups=[securitygroup], TagSpecifications = TAG_SPEC, IamInstanceProfile={'Name': 'ec2-s3'}, UserData=user_data, InstanceInitiatedShutdownBehavior='terminate')
        job_id = event['CodePipeline.job']['id']
        logger.info('Launching ec2 instance ...')
        codepipeline.put_job_success_result(jobId=job_id)

    except Exception as e:
        print("ERROR: " + repr(e))
        message=repr(e)
        traceback.print_exc()
        # Tell CodePipeline we failed
        code_pipeline.put_job_failure_result(jobId=job_id, failureDetails={'message': message, 'type': 'JobFailed'})
