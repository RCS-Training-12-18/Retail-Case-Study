# Run script with the command:
# spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.0 part2.py

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import datetime
import sys
import boto3
import os
import tempfile
from pyspark.sql.functions import col


bucket_name = "rcs-training-12-18"
sc = SparkContext("local[2]", "Case-Study-Part-2")
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("Case-Study-Part-2").getOrCreate()


# Just to show the each section of the program in the middle of all the output
def section_header(h):
    print "\n\n\n"
    print "----------"+h+"----------"
    print "\n\n\n"


# Reads the dataframes from S3 using boto3
# Files are stored in avro format
def read_avro_from_s3():
    section_header("Get avro files from S3")
    s3 = boto3.resource('s3')
    dfs = []
    already_read = []
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        key_parts = key.split("/")
        if key_parts[0] == "raw":
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(obj.get()['Body'].read())
            f.close()
            data = spark.read.format("avro").load(f.name)
            if key_parts[1] in already_read:
                idx = already_read.index(key_parts[1])
                dfs[idx] = (dfs[idx]).union(data)
            else:
                dfs.append(data)
                already_read.append(str(key_parts[1]))
    return dfs, already_read


# Removes all non-sale promotions from all sales dataframes by filtering by promotion ID
def remove_non_prom_sales(dfs, t_order):
    section_header("Removing non-promotion sales")
    for i in range(len(dfs)):
        if t_order[i].startswith("sales"):
            dfs[i] = dfs[i].filter(col('promotion_id') != 0)
    return dfs


# Writes the dataframe to S3 using boto3
# Saves the data as a parquet
def write_parquet2s3(dfs, table_order):
    section_header("Writing Parquet to S3")
    client = boto3.client('s3')
    write_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    for i in range(len(dfs)):
        df = dfs[i]
        dir_name = table_order[i]
        path = os.path.join(tempfile.mkdtemp(), dir_name)
        df.repartition(1).write.format("parquet").save(path)
        parts = 0
        for f in os.listdir(path):
            if f.startswith('part'):
                out = path + "/" + f
                client.put_object(Bucket=bucket_name, Key="cleansed/" + dir_name + "/" + write_time + "/" +
                                                          "%05d.parquet" % (parts,), Body=open(out, 'r'))
                parts += 1


# Joins the sales dataframes and updates the dfs array
def sales_union(dfs, t_order):
    section_header("Join Sales")
    new_dfs = []
    table_order = []
    sales_tables = []
    for i in range(len(dfs)):
        if t_order[i].startswith("sales"):
            sales_tables.append(i)
        else:
            new_dfs.append(dfs[i])
            table_order.append(t_order[i])
    new_dfs.append(dfs[sales_tables[0]])
    for i in range(len(sales_tables)):
        if i != 0:
            new_dfs[len(new_dfs)-1] = new_dfs[len(new_dfs)-1].union(dfs[sales_tables[i]])
    table_order.append("sales")
    return new_dfs, table_order


# Joins the sales and promotion data into one dataframe
def sales_promotion_join(dfs, t_order):
    section_header("Joining sales and promotion tables")
    new_dfs = []
    table_order = []
    sales = t_order.index("sales")
    prom = t_order.index("promotion")
    for i in range(len(dfs)):
        if i != sales and i != prom:
            new_dfs.append(dfs[i])
            table_order.append(t_order[i])
    joined_table = dfs[sales].join(dfs[prom], "promotion_id").drop(dfs[prom].last_update)
    new_dfs.append(joined_table)
    table_order.append("sales")
    return new_dfs, table_order


def main(arg):
    dfs, table_order = read_avro_from_s3()
    dfs = remove_non_prom_sales(dfs, table_order)
    dfs, table_order = sales_union(dfs, table_order)
    dfs, table_order = sales_promotion_join(dfs, table_order)
    write_parquet2s3(dfs, table_order)


# Runs the script
if __name__ == "__main__":
    section_header("Program Start")
    main(sys.argv[1:])
