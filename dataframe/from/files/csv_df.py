from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4,org.apache.spark:spark-avro_2.11:2.4.5') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_file_path = os.path.abspath(current_dir + "/../../../" + "application.yml")

    with open(app_config_file_path) as conf:
        doc = yaml.load(conf, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("\nCreating dataframe from CSV file using 'SparkSession.read.format()'")

    fin_schema = StructType() \
        .add("id", IntegerType(), True) \
        .add("has_debt", BooleanType(), True) \
        .add("has_financial_dependents", BooleanType(), True) \
        .add("has_student_loans", BooleanType(), True) \
        .add("income", DoubleType(), True)

    fin_df = spark.read \
        .option("header", "false") \
        .option("delimiter", ",") \
        .format("csv") \
        .schema(fin_schema) \
        .load("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv")

    fin_df.printSchema()
    fin_df.show()

    print("Creating dataframe from CSV file using 'SparkSession.read.csv()',")

    finance_df = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("s3a://" + doc["s3_conf"]["s3_bucket"] + "/finances.csv") \
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    print("Number of partitions = " + str(fin_df.rdd.getNumPartitions))
    finance_df.printSchema()
    finance_df.show()

    finance_df \
        .repartition(2) \
        .write \
        .partitionBy("id") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "~") \
        .csv("s3a://" + doc["s3_conf"]["s3_bucket"] + "/fin")

    spark.stop()
