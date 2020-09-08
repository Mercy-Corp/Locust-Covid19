import boto3
import paramiko
import botocore
import os

def lambda_handler(event, context):


    # Connect to EC2
    ec2 = boto3.resource('ec2')

    # Get information for all running instances
    running_instances = ec2.instances.filter(Filters=[{
        'Name': 'instance-state-name',
        'Values': ['running']}])

    for instance in running_instances:
       for tag in instance.tags:
          if 'Name'in tag['Key'] and tag['Value'] == 'MercyCorpsTest':
           host = instance.public_ip_address

    BUCKET_NAME = 'mercy-locust-covid19-key' 
    KEY = 'keys/mercy3.pem' 
    BUCKET = os.environ['BUCKET'] 

    s3 = boto3.resource('s3')

    s3.Bucket(BUCKET_NAME).download_file(KEY, '/tmp/mercy3.pem')

    k = paramiko.RSAKey.from_private_key_file("/tmp/mercy3.pem")
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print ("Connecting to " + host)
    c.connect( hostname = host, username = "ec2-user", pkey = k )
    print ("Connected to " + host)

    print ("Running commands ... ")
    commands = [
        "aws s3 cp " + BUCKET + "locustcovid19.zip /home/ec2-user/locustcovid19.zip",
        ]
    for command in commands:
        c.exec_command(command)
        print ("Executing {}".format(command))
        stdin , stdout, stderr = c.exec_command(command)
        print (stdout.read())
        print (stderr.read())

    codepipeline = boto3.client('codepipeline')
    job_id = event['CodePipeline.job']['id']
    codepipeline.put_job_success_result(jobId=job_id)
