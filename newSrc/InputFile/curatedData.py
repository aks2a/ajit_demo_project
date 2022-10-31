import sys
import time
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import re

class curatedData:
    spark = SparkSession.builder.appName("Curated-data").enableHiveSupport().getOrCreate()
    #curatedFile_df = spark.read.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.parquet/part-00000-79d8e7e8-a715-8efg-9e96-e1105ce9bd58-c000.snappy.parquet",header=True)
    curatedFile_df = spark.read.csv("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.csv/part-00000-79d8e7e8-a715-8efg-9e96-e1105ce9bd58-c000.snappy.csv",header=True)




    def __init__(self):
        sc = self.spark.sparkContext

    def fileRead_from_s3(self):
        #self.curatedFile_df = self.spark.read.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.parquet/part-00000-79d8e7e8-a715-8efg-9e96-e1105ce9bd58-c000.snappy.parquet",header=True)
        self.curatedFile_df = self.spark.read.csv(
            "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.csv/part-00000-79d8e7e8-a715-8efg-9e96-e1105ce9bd58-c000.snappy.csv",
            header=True)

    def remove_referer(self):
        self.curatedFile_df = self.curatedFile_df.drop("referer")
        self.curatedFile_df.show(truncate=False)

        # Write to hive
        self.curatedFile_df.write.saveAsTable('curatedLogDetails')
        # write to s3 curated
        self.curatedFile_df.write.parquet(
            "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogFile.csv", mode="overwrite")

    def device_aggregation(self):
        self.curatedFile_df = self.curatedFile_df.withColumn("GET", when(col("method_GET") == "GET", "GET")) \
            .withColumn("HEAD", when(col("method_GET") == "HEAD", "HEAD")) \
            .withColumn("POST", when(col("method_GET") == "POST", "POST")) \
            .withColumn('hour', hour(col('datetime_confirmed')))
        self.curatedFile_df.show(50, truncate=False)

    def perDevice_aggregation(self):
        self.curatedFile_df = self.curatedFile_df.groupBy("clientip").agg(count('GET').alias("GET"), count('POST').alias("POST")
                                                    , count('HEAD').alias("HEAD"), first("hour").alias('hour')
                                                    , count('clientip').alias('no_of_client'))
        self.curatedFile_df.show()
        self.curatedFile_df.repartition(1)

        #print(self.curatedFile_df.rdd.getNumPartitions())
        #self.curatedFile_df.repartition(1).write.mode("overwrite").format("parquet").option("header", "True").save(
            #"C://Users//abhishek.dd//Desktop//Anmol//git//per")

        # Write to hive
        self.curatedFile_df.write.saveAsTable('logPerDevice')

    def write_to_s3_perDevice(self):
        self.curatedFile_df.write.csv("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/logAggPerDevice.csv", mode="overwrite")



    def acrossDevice_aggregation(self):
        self.curatedFile_df = self.curatedFile_df.agg(count('GET').alias("no_get"), count('POST').alias("no_post") \
                                   , count('HEAD').alias("no_head"), first("hour").alias('day_hour'),
                                   count('clientip').alias("no_of_clinets")) \
            .withColumn('row_id', monotonically_increasing_id())
        self.curatedFile_df.show()
        self.curatedFile_df.repartition(1)

        #self.curatedFile_df.write.mode("overwrite").format("parquet").option("header", "True").save(
          #  "C://Users//ajit.ks//Desktop//Anmol//git//across")

        # Write to hive
        self.curatedFile_df.write.saveAsTable('logAcroosDevice')

    def write_to_s3_acrossDevice(self):
        self.curatedFile_df.write.csv("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/logAggAcrossDevice.csv", mode="overwrite")

    # snowflake
    """
    def loadData_snowflake(self):
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        snowflake_database = "aks_DB"
        snowflake_schema = "PUBLIC"
        source_table_name = "AGENTS"
        snowflake_options = {
            "sfUrl": "******.snowflakecomputing.com",
            "sfUser": "********",
            "sfPassword": "********",
            "sfDatabase": "aks_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH"
        }
        self.curatedFile_df = self.curatedFile_df.write.parquet(
            "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogFile.parquet", mode="overwrite")

        
        self.curatedFile_df = self.curatedFile_df.select("id", "clientip", "datetime_confirmed", "method_GET", "status_code", "size", "user_agent",
                        "referer_present")
        self.curatedFile_df.write.format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "curated").mode("overwrite") \
            .save()
        """


if __name__ == "__main__":

    obj1 = curatedData()
    obj1.fileRead_from_s3()
    obj1.remove_referer()
    obj1.device_aggregation()
    obj1.perDevice_aggregation()
    obj1.write_to_s3_perDevice()
    obj1.acrossDevice_aggregation()
    obj1.write_to_s3_acrossDevice()
    #obj1.loadData_snowflake()





