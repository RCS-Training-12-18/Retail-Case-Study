import boto3
import sys
from botocore.exceptions import ClientError
try: 
    s3 = boto3.resource('s3')
    for b in s3.buckets.all():
        print(b.name)
except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidAccessKeyId':
        sys.exit(7)
    if e.response['Error']['Code'] == 'SignatureDoesNotMatch':
        sys.exit(13)
