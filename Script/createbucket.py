import boto3
import sys
s3 = boto3.resource('s3')
try:
    s3.create_bucket(Bucket=sys.argv[1])
    s3.meta.client.head_bucket(Bucket=sys.argv[1])
except:
    sys.exit(3)
