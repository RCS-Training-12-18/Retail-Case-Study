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

#File to save last update time, will move this to S3 later
last_update = "last_update-p1"
raw_out_loc = "file:///home/msr/case-study/raw/"
bucket_name = "rcs-training-12-18"
files = ["promotion", "store", "sales_fact_1997", "sales_fact_1998", "sales_fact_dec_1998", "time_by_day"]

sc = SparkContext("local[2]", "Case-Study-Part-1")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("Case-Study-Part-2").getOrCreate()

# Just to show the each section of the program in the middle of all the output
def section_header(h):
    print "\n\n\n"
    print "----------"+h+"----------"
    print "\n\n\n"


# Writes the dataframe to S3 using boto3
# Saves the data as an avro
def read_avro_from_s3():
    s3 = boto3.resource('s3')
    dfs = []
    already_read = []
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        key_parts = key.split("/")
        if key_parts[1] in files:
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(obj.get()['Body'].read())
            f.close()
            data = spark.read.format("avro").load(f.name)
            if key_parts[1] in already_read:
                idx = already_read.index(key_parts[1])
                dfs[idx] = (dfs[idx]).union(data)
            else:
                dfs.append(data)
                already_read.append(key_parts[1])
    return dfs, already_read


def main(arg):
    dfs, table_order = read_avro_from_s3()
    section_header("Show")
    print len(dfs)
    section_header("Done")



# Runs the script
if __name__ == "__main__":
    section_header("Program Start")
    main(sys.argv[1:])