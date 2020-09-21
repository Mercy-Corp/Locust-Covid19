import boto3
client = boto3.client('s3')

def s3_local(bucket, prefix, localdir):
  
    print(bucket)
    print(prefix)
    print(localdir)
    client.download_file(bucket[5:], prefix, localdir + prefix)

    localpath_in = localdir + prefix 
    print(localpath_in)
    return localpath_in 
