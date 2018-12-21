# Run script with full load:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part1.py F
# To run the script with S3 pushing run
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part1.py F s3

# Run script with incremental load:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part1.py I
# To run the script with S3 pushing run
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part1.py I s3


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import time
import sys
import boto3
import os


#Change these. Later will use a config file
cred_file = "creds"
u = "user"
pw = "password"
url = "jdbc:mysql://localhost/foodmart"
#File to save last update time, will move this to S3 later
last_update = "case-study/last_update-p1"
raw_out_loc = "file:///home/msr/case-study/raw/"
bucket_name = "rcs-training-12-18"

sc = SparkContext("local[2]", "Case-Study-Part-1")
# Toggle for remove local files
# If we can create a temp avro file in python to push to S3, this won't be needed
# For now, default is false and don't use the s3 flag
rem_loc = False

if rem_loc:
    import shutil

# Just to show the start of the program in the middle of all the output
def prog_start():
    print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
    print "----------PROGRAM START----------"
    print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"


def load_dfs():
    # Set up sql context
    sqlContext = SQLContext(sc)
    p_df = sqlContext.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option(
        "dbtable", "promotion").option("user", u).option("password", pw).load()
    s97_df = sqlContext.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option(
        "dbtable", "sales_fact_1997").option("user", u).option("password", pw).load()
    s98_df = sqlContext.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option(
        "dbtable", "sales_fact_1998").option("user", u).option("password", pw).load()
    s98d_df = sqlContext.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option(
        "dbtable", "sales_fact_dec_1998").option("user", u).option("password", pw).load()
    return p_df, s97_df, s98_df, s98d_df


def get_aws_creds():
    f = open(cred_file, 'r')
    return f.read().split(",")


def main(arg):
    # If no arguments in command line, exit the program with an error
    if len(arg) < 1:
        print "No command line arguments."
        exit(1)
    # Try to read the unix timestamp from the file listed in the variable "last_update"
    try:
        f = open(last_update, 'r')
        last_update_unix_ts = f.read().rstrip()
        f.close()
        last_up = True
    except:
        last_up = False
    # Used when updating the "last_update" file
    new_update_time = int(time.time())
    if last_up and arg[0] == 'I':
        # Calls load_dfs() to set the unfiltered dataframes
        prom_df, sales97_df, sales98_df, sales98_dec_df = load_dfs()
        prom_out_df = prom_df.filter(prom_df.last_update > last_update_unix_ts)
        sales97_out_df = sales97_df.filter(sales97_df.last_update > last_update_unix_ts)
        sales98_out_df = sales98_df.filter(sales98_df.last_update > last_update_unix_ts)
        sales98_dec_out_df = sales98_dec_df.filter(sales98_dec_df.last_update > last_update_unix_ts)
    elif arg[0] == 'F':
        # Calls load_dfs() to set the dataframes that will be written. No filtering necessary
        prom_out_df, sales97_out_df, sales98_out_df, sales98_dec_out_df = load_dfs()
    else:
        # Exit because command line input was not able to be parsed
        print "Run failed. Either file: " + last_update + " doesn't exist and an incremental load was attempted or" + \
            "neither F or I was used for the argument."
        exit(1)
    # Saves the dataframes as avro files
    write_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    prom_out_df.write.mode('append').format("avro").save(
        raw_out_loc + "promotion/" + write_time + str(arg[0]))
    sales97_out_df.write.mode('append').format("avro").save(
        raw_out_loc + "97/" + write_time + str(arg[0]))
    sales98_out_df.write.mode('append').format("avro").save(
        raw_out_loc + "98/" + write_time + str(arg[0]))
    sales98_dec_out_df.write.mode('append').format("avro").save(
        raw_out_loc + "98_dec/" + write_time + str(arg[0]))
    # Update the "last_update" file with the newest runtime.
    f = open(last_update, 'w+')
    f.write(str(new_update_time))
    f.close()
    if len(arg) >= 2 and (arg[1] == "s3" or arg[1] == "S3"):
        client = boto3.client('s3')
        for f in os.listdir(raw_out_loc[7:] + "promotion/" + write_time + str(arg[0])):
            if f.startswith('part'):
                prom_out = raw_out_loc[7:] + "promotion/" + write_time + str(arg[0]) + "/" + f
        client.put_object(Bucket = bucket_name, Key = "raw/promotion/"+write_time+str(arg[0])+".arvo",
                          Body = open(prom_out, 'r'))
        for f in os.listdir(raw_out_loc[7:] + "97/" + write_time + str(arg[0])):
            if f.startswith('part'):
                f97_out = raw_out_loc[7:] + "97/" + write_time + str(arg[0]) + "/" + f
        client.put_object(Bucket = bucket_name, Key = "raw/97/"+write_time+str(arg[0])+".arvo",
                          Body = open(f97_out, 'r'))
        for f in os.listdir(raw_out_loc[7:] + "98/" + write_time + str(arg[0])):
            if f.startswith('part'):
                f98_out = raw_out_loc[7:] + "98/" + write_time + str(arg[0]) + "/" + f
        client.put_object(Bucket = bucket_name, Key = "raw/98/"+write_time+str(arg[0])+".arvo",
                          Body = open(f98_out, 'r'))
        for f in os.listdir(raw_out_loc[7:] + "98_dec/" + write_time + str(arg[0])):
            if f.startswith('part'):
                f98d_out = raw_out_loc[7:] + "98_dec/" + write_time + str(arg[0]) + "/" + f
        client.put_object(Bucket = bucket_name, Key = "raw/98d/"+write_time+str(arg[0])+".arvo",
                          Body = open(f98d_out, 'r'))
    if rem_loc:
        shutil.rmtree(raw_out_loc[7:])
# Runs the script
if __name__ == "__main__":
    prog_start()
    main(sys.argv[1:])
