from pyspark.sql import SparkSession,Row,Window
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml
from src.model.Product import Product

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create the SparkSession
    sparkSession = SparkSession \
        .builder \
        .appName("DSL examples") \
        .master('local[*]') \
        .getOrCreate()

    current_dir = os.path.abspath(os.path.dirname(__file__))
    appConfigFilePath = os.path.abspath(current_dir + "/../../../"+"application.yml")

    with open(appConfigFilePath) as conf:
        doc = yaml.load(conf,Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", doc["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", doc["s3_conf"]["secret_access_key"])
    hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    finFilePath = "s3a://"+doc["s3_conf"]["s3_bucket"]+"/finances-small"
    financeDf = sparkSession.read.parquet(finFilePath)
    financeDf.createOrReplaceTempView("raw_finances")

    sparkSession.sql("""
          select
            AccountNumber,
            Amount,
            to_date(cast(unix_timestamp(Date, 'MM/dd/yyyy') as timestamp)) as Date,
            Description
          from
            raw_finances
          """)\
        .createOrReplaceTempView("finances")

    financeDf.printSchema()

    sparkSession.sql("""
          select
            AccountNumber,
            Amount,
            Date,
            Description,
            avg(Amount) over (partition by AccountNumber order by Date rows between 4 preceding and 0 following) as RollingAvg
          from
            finances
          """)\
        .show(5,False)

    #Row can be used as case class in pyspark
    productList = [
        Row(product="Thin",category= "Cell phone",revenue=6000),
        Row(product="Normal", category= "Tablet", revenue=1500),
        Row(product="Mini", category= "Tablet", revenue=5500),
        Row(product="Ultra Thin", category= "Cell phone",revenue= 5000),
        Row(product="Very Thin", category= "Cell phone", revenue=6000),
        Row(product="Big", category= "Tablet", revenue=2500),
        Row(product="Bendable", category= "Cell phone", revenue=3000),
        Row(product="Foldable", category= "Cell phone", revenue=3000),
        Row(product="Pro", category= "Tablet", revenue=4500),
        Row(product="Pro2", category= "Tablet",revenue= 6500)
                  ]

    productList1= [
        Product("Thin", "Cell phone", 6000),
        Product("Normal", "Tablet", 1500),
        Product("Mini", "Tablet", 5500),
        Product("Ultra Thin", "Cell phone", 5000),
        Product("Very Thin", "Cell phone", 6000),
        Product("Big", "Tablet", 2500),
        Product("Bendable", "Cell phone", 3000),
        Product("Foldable", "Cell phone", 3000),
        Product("Pro", "Tablet", 4500),
        Product("Pro2", "Tablet", 6500)
    ]
    products = sparkSession.createDataFrame(productList1)
    products.createOrReplaceTempView("products")
    products.printSchema()

    catRevenueWindowSpec = Window.partitionBy("category")\
        .orderBy("revenue")

    sparkSession.sql("""
            select
              product,
              category,
              revenue,
              lag(revenue, 1) over (partition by category order by revenue) as prevRevenue,
              lag(revenue, 2, 0) over(partition by category order by revenue) as prev2Revenue,
              row_number() over (partition by category order by revenue) as row_number,
              rank() over(partition by category order by revenue) as rev_rank,
              dense_rank() over(partition by category order by revenue) as rev_dense_rank
             from
              products
          """)\
        .show(5,False)

#    sparkSession\
#        .sql("""
#                    select
#                      *,
#                      window(Date, '30 days', '15 minutes')
#                    from
#                      finances
#              """)\
#        .show(5,False)
