# -*- coding: utf-8 -*-
"""
The aim of this module is to stop the EC2 instance.

@author: rashmi.upreti@accenture.com
"""

import boto3
region = 'us-east-1'
instances = ['i-0fce932d990871605']
ec2 = boto3.client('ec2', region_name=region)

ec2.stop_instances(InstanceIds=instances)







