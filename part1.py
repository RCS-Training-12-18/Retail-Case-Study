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
import tempfile

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


# Just to show the start of the program in the middle of all the output
def prog_start():
    print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
    print "----------PROGRAM START----------"
    print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"


def load_df(table_name, incremental,ts):
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("jdbc").option("url", url).option("driver", "com.mysql.jdbc.Driver").option(
          "dbtable", table_name).option("user", u).option("password", pw).load()
    if incremental == 'I':
        return df.filter(df.last_update > ts)
    else:
        return df


def write_avro2s3(df, dir_name, write_time, incremental):
    client = boto3.client('s3')
    path = os.path.join(tempfile.mkdtemp(), dir_name)
    df.write.format("avro").save(path)
    for f in os.listdir(path):
        if f.startswith('part'):
            out = path + "/" + f
    client.put_object(Bucket=bucket_name, Key="raw/" + dir_name + "/" +
                                              write_time + str(incremental) + ".arvo",
                      Body=open(out, 'r'))


def write_df(df, table_name, w_time, incremental):
    df.write.mode('append').format("avro").save(
        raw_out_loc + table_name + "/" + w_time + str(incremental))


def main(arg):
    # If no arguments in command line, exit the program with an error
    if len(arg) < 1:
        print "No command line arguments."
        exit(1)
    if arg[0] != 'I' and arg[0] != 'F':
        print "Bad argument. Use 'I' or 'F'"
        exit(1)
    # Try to read the unix timestamp from the file listed in the variable "last_update"
    try:
        f = open(last_update, 'r')
        last_update_unix_ts = f.read().rstrip()
        f.close()
    except:
        if arg[0] == 'I':
            print "Failed to load file:" + last_update
            exit(1)
        last_update_unix_ts = 0
    # Used when updating the "last_update" file
    new_update_time = int(time.time())
    prom_df = load_df("promotion", arg[0],last_update_unix_ts)
    s97_df = load_df("sales_fact_1997",arg[0],last_update_unix_ts)
    s98_df = load_df("sales_fact_1998",arg[0],last_update_unix_ts)
    s98d_df = load_df("sales_fact_dec_1998",arg[0],last_update_unix_ts)
    write_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # Update the "last_update" file with the newest runtime.
    f = open(last_update, 'w+')
    f.write(str(new_update_time))
    f.close()
    # Saves the dataframes as avro files in S3
    if len(arg) >= 2 and (arg[1] == "s3" or arg[1] == "S3"):
        write_avro2s3(prom_df, "promotion", write_time, arg[0])
        write_avro2s3(s97_df, "s97", write_time, arg[0])
        write_avro2s3(s98_df, "s98", write_time, arg[0])
        write_avro2s3(s98d_df, "s98d", write_time, arg[0])
    # Saves the dataframes as avro files locally
    else:
        write_df(prom_df, "promotion", write_time, arg[0])
        write_df(s97_df, "s97", write_time, arg[0])
        write_df(s98_df, "s98", write_time, arg[0])
        write_df(s98d_df, "s98d", write_time, arg[0])


# Runs the script
if __name__ == "__main__":
    prog_start()
    main(sys.argv[1:])
