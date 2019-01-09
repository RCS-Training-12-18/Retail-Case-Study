# This is written for Python 2.7

# Run script with the command:
# spark-submit part3.py

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import sys
import boto3
import os
import tempfile
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as sf

bucket_name = "rcs-training-12-18"
sc = SparkContext("local[2]", "Case-Study-Part-3")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("Case-Study-Part-3").getOrCreate()


# Just to show the each section of the program in the middle of all the output
def section_header(h):
    print "\n\n\n"
    print "----------"+h+"----------"
    print "\n\n\n"


# Reads the dataframes from S3 using boto3
# Data is stored as parquet before being read in
def read_parquet_from_s3():
    section_header("Get parquet files from S3")
    s3 = boto3.resource('s3')
    dfs = []
    already_read = []
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        key_parts = key.split("/")
        if key_parts[0] == "curated":
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(obj.get()['Body'].read())
            f.close()
            data = spark.read.format("parquet").load(f.name)
            if key_parts[1] in already_read:
                idx = already_read.index(key_parts[1])
                dfs[idx] = (dfs[idx]).union(data)
            else:
                dfs.append(data)
                already_read.append(str(key_parts[1]))
    return dfs, already_read


# Writes the dataframe to S3 using boto3
# Saves the data as a csv
def write_csv2s3(df):
    section_header("Writing CSV to S3")
    client = boto3.client('s3')
    write_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dir_name = "sales"
    path = os.path.join(tempfile.mkdtemp(), dir_name)
    df.repartition(1).write.format("csv").save(path)
    parts = 0
    for f in os.listdir(path):
        if f.startswith('part'):
            out = path + "/" + f
            client.put_object(Bucket=bucket_name, Key="staging/" + dir_name + "/" +
                                                      "%05d.csv" % (parts,), Body=open(out, 'r'))
            parts += 1


# Merge the dataframes into 1 dataframe
def join_ini_dataframes(dfs, table_order):
    section_header("Join dataframes")
    sales = table_order.index("sales")
    tbd = table_order.index("time_by_day")
    store = table_order.index("store")
    df = dfs[sales].join(dfs[tbd], "time_id").join(dfs[store], "store_id")
    # Remove unused rows
    df = df.select("region_id", "promotion_id", "cost", "store_sales", "the_day", "the_month", "the_year",
                   dfs[store].region_id)
    return df


# Split the dataframe into weekend and weekday values
def split_into_weekend_weekday(df):
    section_header("Weekday and weekend split")
    wkend = df.filter("the_day == 'Saturday'").union(df.filter("the_day == 'Sunday'"))
    wkend = wkend.drop("the_day")
    wkday = (df.filter("the_day != 'Saturday'")).filter("the_day != 'Sunday'")
    wkday = wkday.drop("the_day")
    return [wkday, wkend]


# Aggregate the sales for weekend and weekday based on region ID, promotion ID, the year and the month
# Group by also includes cost to keep that row
def aggregate_sales(dfs):
    section_header("Sum up sales")
    new_dfs = []
    for i in range(len(dfs)):
        dfs[i] = dfs[i].withColumn("sales", dfs[i]["store_sales"].cast(DoubleType())).drop("store_sales")
    new_dfs.append(dfs[0].groupby(
        "region_id", "promotion_id", "the_year", "the_month", "cost").agg(sf.sum("sales").alias("wkday")))
    new_dfs.append(dfs[1].groupby(
        "region_id", "promotion_id", "the_year", "the_month", "cost").agg(sf.sum("sales").alias("wkend")))
    return new_dfs


# Join the weekday and weekend sales data (actually uses a union) and combines them into the same row
def join_weekend_weekday(dfs):
    section_header("Merge weekend and weekday back into one dataframe")
    # Create matching dataframes for a union
    dfs[0] = dfs[0].withColumn("wkend", sf.lit(0))
    dfs[1] = dfs[1].withColumn("wkday", sf.lit(0))
    # Reorder dfs[1] so that the columns match up for the union
    dfs[1] = dfs[1].select("region_id", "promotion_id", "the_year", "the_month", "cost", "wkday", "wkend")
    df = dfs[0].union(dfs[1])
    # Group the matching values for weekday and weekend into the same row
    df = df.groupby("region_id", "promotion_id", "the_year", "the_month", "cost").agg(sf.sum("wkday").alias("wkday"),
                                                                                      sf.sum("wkend").alias("wkend"))
    return df


def main():
    dfs, table_order = read_parquet_from_s3()
    df = join_ini_dataframes(dfs, table_order)
    dfs = split_into_weekend_weekday(df)
    dfs = aggregate_sales(dfs)
    df = join_weekend_weekday(dfs)
    write_csv2s3(df)


# Runs the script
if __name__ == "__main__":
    section_header("Program Start")
    main()
