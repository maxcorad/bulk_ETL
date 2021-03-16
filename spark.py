# crear SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ETL").enableHiveSupport().getOrCreate()