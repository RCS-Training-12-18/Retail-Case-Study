# Run script by using:
# spark-submit --packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0 part1.py F

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import time
import sys

u = "user"
pw = "password"
url = "jdbc:mysql://localhost/foodmart"
sc = SparkContext("local[2]", "NetworkWordCount")
last_update = "case-study/last_update-p1"
raw_out_loc = "file:///home/msr/case-study/raw/"

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
        prom_out_df = prom_df.filter(prom_table.last_update > last_update_unix_ts)
        sales97_out_df = sales97_df.filter(sales97_table.last_update > last_update_unix_ts)
        sales98_out_df = sales98_df.filter(sales98_table.last_update > last_update_unix_ts)
        sales98_dec_out_df = sales98_dec_df.filter(sales98_dec_table.last_update > last_update_unix_ts)
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

# Runs the script
if __name__ == "__main__":
    prog_start()
    main(sys.argv[1:])
