import boto3
import sys
from os.path import expanduser
home = expanduser("~")
client = boto3.client('s3')
client.put_object(Bucket=sys.argv[1], Key="config_files/tables", Body=open(home+"/Retail-Case-Study/tables"))
