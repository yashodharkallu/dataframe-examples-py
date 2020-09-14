from pyspark.sql import SparkSession
import yaml
import os.path
import src.utils.aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
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

    #dbtable" : "(select a, b, c from testdb.TRANSACTIONSYNC where some_cond) as t"
    jdbcParams = {  "url": ut.getMysqlJdbcUrl(doc),
                    "lowerBound" : "1",
                    "upperBound" : "100",
                    "dbtable" : "IRMSDB.TRANSACTIONSYNC",
                    "numPartitions" :"2",
                    "partitionColumn" :"App_Transaction_Id",
                    "user" : doc["mysql_conf"]["username"],
                    "password" : doc["mysql_conf"]["password"]
                }
    print(jdbcParams)

    #use the ** operator/un-packer to treat a python dictionary as **kwargs
    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txnDF = sparkSession\
        .read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .options(**jdbcParams)\
        .load()

    txnDF.show()
