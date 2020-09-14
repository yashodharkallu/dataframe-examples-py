from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
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
