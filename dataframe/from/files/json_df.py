from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
import os.path
import yaml

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .master('local[*]') \
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

    company_df = spark.read\
        .json("s3a://" + doc["s3_conf"]["s3_bucket"] + "/company.json")

    company_df.printSchema()
    company_df.show(5, False)

    flattened_df = company_df.select(col("company"), explode(col("employees")).alias("employee"))
    flattened_df.show()

    flattened_df \
        .select(col("company"), col("employee.firstName").alias("emp_name")) \
        .show()

    spark.stop()
