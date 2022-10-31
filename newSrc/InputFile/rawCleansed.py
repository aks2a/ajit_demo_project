import sys
import time
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import re

class rawCleansed:
    spark = SparkSession.builder.appName("Curated-Layer").enableHiveSupport().getOrCreate()
    #cleanFile_df = spark.read.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.parquet/part-00000-76yj84k1-7y4t-9in5-9e96-n45f7sdr62f45rt-c000.snappy.parquet",header=True)
    cleanFile_df = spark.read.csv(
        "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.csv/part-00000-79d8e7e8-a612-4abe-9e96-e8207ce9bd58-c000.csv",
        header=True)

    def __init__(self):
        sc = self.spark.sparkContext


    def fileRead_from_s3(self):

            #self.cleanFile_df = self.spark.read.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.parquet/part-00000-76yj84k1-7y4t-9in5-9e96-n45f7sdr62f45rt-c000.snappy.parquet",header=True)
            self.cleanFile_df = spark.read.csv(
                "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.csv/part-00000-79d8e7e8-a612-4abe-9e96-e8207ce9bd58-c000.csv",
                header=True)

    #Date time Format mm-dd-yyyy hh24:mi:ss
    def datetime_formats(self):
        self.cleanFile_df = self.cleanFile_df.withColumn("datetime", split(self.cleanFile_df["datetime"], ' ').getItem(0)).withColumn("datetime",to_timestamp("datetime",'dd/MMM/yyyy:hh:mm:ss'))\
                                                                                                                .withColumn("datetime",to_timestamp("datetime",'MMM/dd/yyyy:hh:mm:ss'))
        # self.cleanFile_df.show()

    def referer_present(self):
        self.cleanFile_df = self.cleanFile_df.withColumn("referer_present(Y/N)",
                                      when(col("referer") == None, "N") \
                                      .otherwise("Y"))
        self.cleanFile_df.show()

    def write_to_s3(self):
        #self.cleanFile_df.write.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.parquet", mode="overwrite")
        self.cleanFile_df.write.csv("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/curatedLogDetails.csv", mode="overwrite")

    def write_to_hive(self):
        pass
        self.cleanFile_df.write.saveAsTable('curatedLogDetails')


if __name__ == "__main__":

    obj1 = rawCleansed()
    obj1.fileRead_from_s3()
    obj1.datetime_formats()
    obj1.referer_present()
    obj1.write_to_s3()
    obj1.write_to_hive()

