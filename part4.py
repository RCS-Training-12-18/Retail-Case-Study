# This is written for Python 2.7

# Run script with the command:
# python part4.py


import datetime
import boto3
import sys
from dateutil import tz
from dateutil.tz import *
from boto3 import Session
import snowflake.connector

bucket_name = "rcs-training-12-18"
dt_format = "%Y%m%d_%H%M%S %Z"
creds = "/home/msr/snowflake_creds"
last_update = "sf_last_update"


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
        if key_parts[0] == "config_files" and key_parts[1] == last_update:
            return obj.get()['LastModified']
    # Default if time doesn't exist
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


# Writes the dataframe to S3 using boto3
# Saves the data as a csv
def write_last_update_to_s3():
    section_header("Writing last update to S3")
    client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, 'config_files/'+last_update).delete()
    client.put_object(Bucket=bucket_name, Key="config_files/"+last_update, Body="")


def load_sf_creds():
    f = open(creds, 'r')
    user = f.readline().rstrip()
    password = f.readline().rstrip()
    account = f.readline().rstrip()
    f.close()
    return user, password, account


def load_csv_in_snowflake(folders):
    if len(folders) == 0:
        section_header("No data to move to Snowflake")
        return
    section_header("Moving Data to Snowflake")
    session = Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    u, p, a = load_sf_creds()
    con = snowflake.connector.connect(
        user=u,
        password=p,
        account=a,
    )
    con.cursor().execute("USE RCS")
    for f in folders:
        con.cursor().execute("""
        COPY INTO sales FROM s3://""" + bucket_name + "/" + f + """
            CREDENTIALS = (
                aws_key_id='{aws_access_key_id}',
                aws_secret_key='{aws_secret_access_key}')
            FILE_FORMAT=(field_delimiter=',')
        """.format(
            aws_access_key_id=current_credentials.access_key,
            aws_secret_access_key=current_credentials.secret_key))


def main():
    folders = since_last_update_s3()
    write_last_update_to_s3()
    load_csv_in_snowflake(folders)


# Runs the script
if __name__ == "__main__":
    section_header("Program Start")
    main()
