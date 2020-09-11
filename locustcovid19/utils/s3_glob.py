import boto3
client = boto3.client('s3')

def s3_glob(bucket, prefix, pattern):
     
    resp = client.list_objects_v2(Bucket=bucket[5:], Prefix=prefix)
    keys = []
    all_files = []
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    for i in keys:
        if pattern in i:
           s = bucket + '/' + str(i)
           all_files.append(s)
    return all_files
