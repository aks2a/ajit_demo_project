import sys
import time
#from pyspark import *

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import re

class RawLayer:
    spark = SparkSession.builder.appName("Cleasened-layer").enableHiveSupport().getOrCreate()
    rawFile_df = spark.read.text("s3://mkc-tutorial-destination-bucket-ajit/tutorial/training_dataProject.txt")




    # spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()
    def __init__(self):
        sc = self.spark.sparkContext


    def fileRead_from_s3(self):

            self.rawFile_df = self.spark.read.text("s3://mkc-tutorial-destination-bucket-ajit/tutorial/training_dataProject.txt")

    def raw_Extract_Data(self):
        ### CREATING THE REGULAR EXP PATTERNS FOR EXTRACTING THE REQUIRE DATA
        # Extracting IP data
        data_Pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        # Extract TimeStamp
        time_Pattern = r'(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})'
        # Get Method Column
        gm = r'GET|POST|HEAD'
        # URL Extractions
        u_Patt = r'\s\S+\sHTTP/1.1"'
        # status Pattern
        s_Pattern = r'\s(\d{3})\s'
        # Content Size
        cs_Pattern = r'\s(\d+)\s"'
        # referer value
        rv = r'("https(\S+)")'
        # User Agent
        u_Agent = r'(Mozilla|Dalvik|Goog|torob|Bar)(\S+\s+)*'

        ### ARRANGING THE EXTRACTING DATA INTO DATAFRAME
        self.rawFile_df = self.rawFile_df.withColumn("id", monotonically_increasing_id()) \
            .select('id', regexp_extract('value', data_Pattern, 1).alias('client/ip')
                    , regexp_extract('value', time_Pattern, 1).alias('datetime_confirmed')
                    , regexp_extract('value', gm, 0).alias("method_GET")
                    , regexp_extract('value', u_Patt, 0).alias('request')
                    , regexp_extract('value', s_Pattern, 1).alias('status_code')
                    , regexp_extract('value', cs_Pattern, 0).alias('size')
                    , regexp_extract('value', rv, 2).alias('referer')
                    , regexp_extract('value', u_Agent, 0).alias('user_agent'))
        self.rawFile_df.show(truncate=False)

    def delete_raw_special_Char(self):
        # delete any special characters in the request column(% ,- ? =)
        self.rawFile_df = self.rawFile_df.withColumn('request', regexp_replace('request', '%|-|\?=', ''))

    def size_to_kb(self):
        self.rawFile_df = self.rawFile_df.withColumn('size', round(self.rawFile_df.size / 1024, 2))
        return self.rawFile_df

    def remove_empty_null(self):
        self.rawFile_df = self.rawFile_df.select(
            [when(col(c) == "-", None).otherwise(col(c)).alias(c) for c in self.rawFile_df.columns])
        self.rawFile_df.show()
        return self.rawFile_df

    def count_null_row_wise(self):
        return self.rawFile_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.rawFile_df.columns])


    def save_To_S3(self):
        #self.rawFile_df.write.parquet("s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.parquet", mode="overwrite")
        self.rawFile_df.write.csv(
            "s3://mkc-tutorial-destination-bucket-ajit/tutorial/extractData/cleansedLogDetails.csv",
            mode="overwrite")


    def save_To_hive(self):
       self.rawFile_df.write.saveAsTable('cleansedLogData')


if __name__ == "__main__":


    # create Objects
    obj1= RawLayer()

    # call RawLayer functions
    obj1.raw_Extract_Data()
    obj1.delete_raw_special_Char()
    obj1.size_to_kb()
    obj1.remove_empty_null()
    obj1.count_null_row_wise()
    obj1.save_To_S3()
    obj1.save_To_hive()