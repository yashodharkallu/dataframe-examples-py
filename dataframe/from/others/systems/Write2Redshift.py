from pyspark.sql import SparkSession
import yaml
import os.path
import src.utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("Read from enterprise applications") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    print("\nCreating Dataframe from txn_fact dataset,")
    txnDf = sparkSession.read\
        .option("header","true")\
        .option("delimiter", "|")\
        .csv("s3a://"+doc["s3_conf"]["s3_bucket"]+"/txn_fct.csv")

    txnDf.show(5,False)

    print("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")

    jdbcUrl = ut.getRedshiftJdbcUrl(doc)
    print(jdbcUrl)

    txnDf.write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", jdbcUrl) \
        .option("tempdir", "s3a://"+doc["s3_conf"]["s3_bucket"]+"/temp") \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", "PUBLIC.TXN_FCT") \
        .mode("overwrite")\
        .save()

    print("Completed   <<<<<<<<<")

