from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os.path
import yaml
import sys

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file_path = os.path.abspath(current_dir + "/../../../" + "application.yml")

    with open(app_config_file_path) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Read access key and secret key from cmd line argument
    s3_access_key = doc["s3_conf"]["access_key"]
    s3_secret_access_key = doc["s3_conf"]["secret_access_key"]
    if len(sys.argv) > 1 and sys.argv[1] is not None and sys.argv[2] is not None:
        s3_access_key = sys.argv[1]
        s3_secret_access_key = sys.argv[2]

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", s3_access_key)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("\nCreating dataframe from parquet file using 'SparkSession.read.parquet()',")
    nyc_omo_df = spark.read \
        .parquet("s3a://" + doc["s3_conf"]["s3_bucket"] + "/NYC_OMO") \
        .repartition(5)

    print("# of records = " + str(nyc_omo_df.count()))
    print("# of partitions = " + str(nyc_omo_df.rdd.getNumPartitions))

    nyc_omo_df.printSchema()

    print("Summery of NYC Open Market Order (OMO) charges dataset,")
    nyc_omo_df.describe().show()

    print("OMO frequency distribution of different Boroughs,")
    nyc_omo_df.groupBy("Boro") \
        .agg({"Boro": "count"}) \
        .withColumnRenamed("count(Boro)", "OrderFrequency") \
        .show()

    print("OMO's Zip & Borough list,")

    boro_zip_df = nyc_omo_df \
        .select("Boro", nyc_omo_df["Zip"].cast(IntegerType())) \
        .groupBy("Boro") \
        .agg({"Zip": "collect_set"}) \
        .withColumnRenamed("collect_set(Zip)", "ZipList") \
        .withColumn("ZipCount", F.size("ZipList"))

    boro_zip_df \
        .select("Boro", "ZipCount", "ZipList") \
        .show(5)

    # Window functions
    window_spec = Window.partitionBy("OMOCreateDate")
    omo_daily_freq = nyc_omo_df \
        .withColumn("OMODailyFreq", F.count("OMOID").over(window_spec).alias("OMODailyFreq"))

    print("# of partitions in window'ed OM dataframe = " + str(omo_daily_freq.count()))
    omo_daily_freq.show(5)

    omo_daily_freq.select("OMOCreateDate", "OMODailyFreq") \
        .distinct() \
        .show(5)

    omo_daily_freq \
        .repartition(10) \
        .write \
        .mode("overwrite") \
        .parquet("s3a://" + doc["s3_conf"]["s3_bucket"] + "/nyc_omo_data")

    spark.stop()
