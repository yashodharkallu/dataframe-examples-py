from pyspark.sql import SparkSession
import yaml
import os.path

if __name__ == '__main__':
    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .master('local[*]') \
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    olTxnDf=sparkSession.read\
        .format("com.springml.spark.sftp")\
        .option("host", doc["sftp_conf"]["hostname"])\
        .option("port", doc["sftp_conf"]["port"])\
        .option("username", doc["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir + "/../../../../"+doc["sftp_conf"]["pem"]))\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(doc["sftp_conf"]["directory"]+"/receipts_delta_GBR_14_10_2017.csv")

    olTxnDf.show(5,False)
