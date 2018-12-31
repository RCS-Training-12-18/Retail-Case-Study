# Run script with the command:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part4.py

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import sys
import boto3
import os
import tempfile
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as sf
from dateutil import tz

bucket_name = "rcs-training-12-18"
sc = SparkContext("local[2]", "Case-Study-Part-3")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("Case-Study-Part-3").getOrCreate()


# Just to show the each section of the program in the middle of all the output
def section_header(h):
    print "\n\n\n"
    print "----------"+h+"----------"
    print "\n\n\n"


# Loads last time data was pushed to S3 and returns it, otherwise returns a time before the program started running
def last_snowflake_push():
    section_header("Getting last push to Snowflake time")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        key_parts = key.split("/")
        if key_parts[0] == "config_files" and key_parts[1] == "last_sf_update":
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(obj.get()['Body'].read())
            f.close()
            file = open(f.name, 'r')
            last_t = file.readline()
            file.close()
            return last_t
    return datetime.datetime.strptime("Dec 25, 2018 00:00:00", "%b %d, %Y %H:%M:%S").replace(tzinfo=tz.tzutc())


# Finds all folders that are newer that the most recent push to S3
def since_last_update_s3():
    last_time = last_snowflake_push()
    section_header("Finding folders that haven't been updated to Snowflake")
    s3 = boto3.resource('s3')
    folders = []
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        key_parts = key.split("/")
        if key_parts[0] == "staging":
            if obj.get()['LastModified'] > last_time:
                folder = ""
                for i in range(len(key_parts)-1):
                    folder = folder + key_parts[i] + '/'
                folders.append(folder)
    return folders


def main(arg):
    for v in since_last_update_s3():
        print v


# Runs the script
if __name__ == "__main__":
    section_header("Program Start")
    main(sys.argv[1:])
