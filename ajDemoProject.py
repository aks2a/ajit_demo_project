from pyspark.sql import *
from pyspark.sql.functions import *
import boto3

### BUILDING THE SPARK SESSION
spark = SparkSession.builder.master('local').appName('unstructured').enableHiveSupport().getOrCreate()
data = 'C:\Users\ajit.ks\Desktop\training_dataProject.txt'

df = spark.read.format('text').load(data)

Access_key_ID="*****"
Secret_access_key="***********"

### ENABLE HADOOP S3A SETTINGS(SIMPLY CONFIG NOT TO GO IN HDFS FOR DATA GO IN S3)
spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                     "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",Access_key_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",Secret_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")


### CREATING THE REGULAR EXP PATTERNS FOR EXTRACTING THE REQUIRE DATA
# Extracting IP data
data_Pattern = r'(^\S+\.[\S+\.]+\S+)\s'
# Extract TimeStamp
time_Pattern = r'(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})' 
# Get Method Column 
gm=r'GET|POST|HEAD'
# URL Extractions 
u_Patt=r'\s\S+\sHTTP/1.1"'
# status Pattern 
s_Pattern = r'\s(\d{3})\s'
# Content Size
cs_Pattern = r'\s(\d+)\s"'
# referer value
rv=r'("https(\S+)")'
# User Agent 
u_Agent=r'(Mozilla|Dalvik|Goog|torob|Bar)(\S+\s+)*'


### ARRANGING THE EXTRACTING DATA INTO DATAFRAME
raw_df=df.withColumn("id",monotonically_increasing_id())\
      .select('id',regexp_extract('value',data_Pattern,1).alias('client/ip')
      ,regexp_extract('value', time_Pattern,1).alias('datetime_confirmed')
      ,regexp_extract('value',gm,0).alias("method_GET")
      ,regexp_extract('value',u_Patt,0).alias('request')
      ,regexp_extract('value',s_Pattern,1).alias('status_code')
      ,regexp_extract('value',cs_Pattern,0).alias('size')
      ,regexp_extract('value',rv,2).alias('referer')
      ,regexp_extract('value',u_Agent,0).alias('user_agent')) 
raw_df.show(truncate=False)

### CLEANING THE DATA WITH DATATYPE AND ADDING NEW COLUMN
cleansed = raw_df.withColumn("id", col('id').cast('int')) \
    .withColumn("datetime_confirmed",to_timestamp("datetime_confirmed",'dd/MMM/yyyy:HH:mm:ss')) \
    .withColumn('status_code', col('status_code').cast('int')) \
    .withColumn('size', col('size').cast('int'))\
    .withColumn('referer_present(Y/N)',when(col('referer') == '','N')\
             .otherwise('Y'))
cleansed.printSchema()
cleansed.show()

### EXTRACTING THE DAVICE FROM THE AGENTS
device_pt = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'
curated=cleansed.drop('referer')
curated.show()


#curated.write.format('csv').mode('overwrite').save('E:/demoDatasets/Curated/') 
device_curated = curated

### SAVING THE DATA INTO S3 BUCKET THROUGH BOTO3

bucket_name="ajitdswp"
data_path="E:\demoDatasets\Curated\part-00000-808dbcb9-8085-40f0-81a0-920680b5342f-c000.csv"
s3 = boto3.resource('s3')
s3.meta.client.upload_file(data_path, bucket_name, 'curated')
print("File successfully uploaded")

device_curated = device_curated.withColumn('device',regexp_extract(col('user_agent'),device_pt,2))
device_curated.show()

### ARRANGING THE DATA FOR AGG PER DEVICE AND ACROSS DEVICE
device_agg= device_curated.withColumn("GET",when(col("method_GET")=="GET","GET"))\
                          .withColumn("HEAD",when(col("method_GET")=="HEAD","HEAD"))\
                           .withColumn("POST",when(col("method_GET")=="POST","POST"))\
                            .withColumn('hour', hour(col('datetime_confirmed')))

per_de=device_agg.groupBy("device").agg(count('GET').alias("GET"),count('POST').alias("POST") 
                                 ,count('HEAD').alias("HEAD"),sum("hour").alias('hour')
                                 ,count('client/ip'))
per_de.show()
across_de=device_agg.agg(count('GET').alias("no_get"),count('POST').alias("no_post"),count('HEAD').alias("no_head"),sum("hour").alias('day_hour'),count('client/ip').alias("no_of_clinets"))\
.withColumn('row_id',monotonically_increasing_id())
across_de.show()

### CACHE THE DATA
per_device=per_de.cache()
across_device=across_de.cache()

### WRITTING THE CURATED DATASET INTO HIVE TABLES
per_device.write.format('ORC').mode('overwrite').saveAsTable('log_agg_per_device')
across_device.write.format('ORC').mode('overwrite').saveAsTable('log_agg_across_device')
spark.sql('show databases;').show()
spark.sql('show tables;').show()


### WRITING THE TABLE IN SNOWFLAKE 
"""
def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "curated"
    snowflake_schema = "public"
    target_table_name = "curatedtbl"
    snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "******",
        "sfPassword": "**************",
        "sfDatabase": CURATEDS3,
        "sfSchema": CURATEDS3_schema,
        "sfWarehouse": "curatedS3_snowflake"
    }

    curated.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "curated").mode("overwrite") \
        .save()

    df_across.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "df_perdevice").mode("overwrite") \
        .save()

    df_hours_getposthead_new.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "df_across").mode("overwrite") \
        .save()


main()
""" 
