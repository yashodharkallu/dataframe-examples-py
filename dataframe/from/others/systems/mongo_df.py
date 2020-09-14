from pyspark.sql import SparkSession
import yaml
import os.path

if __name__ == '__main__':
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

    sparkSession.conf.set("spark.mongodb.input.uri",doc["mongodb_config"]["input.uri"])

    students = sparkSession\
        .read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("database", doc["mongodb_config"]["input.database"])\
        .option("collection", doc["mongodb_config"]["collection"])\
        .load()

    students.show()