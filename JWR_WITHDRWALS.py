spark-submit --master yarn --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3,com.databricks:spark-redshift_2.11:2.0.1,org.apache.spark:spark-avro_2.11:2.4.0 --jars /tmp/RedshiftJDBC41-1.1.10.1010.jar,/home/hadoop/mssql-jdbc-7.2.1.jre8.jar  --conf spark.dynamicAllocation.maxExecutors=4 --executor-memory 2g --executor-cores 2 --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC"   GET_FULL_GAMESTATS2.py



import os

from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
import os
import time as t
import shutil
import boto3
#hdfs://ip-172-31-0-116.ap-south-1.compute.internal:8020/users_test
pkey = "userid"
s3 = boto3.resource("s3")
bucket = s3.Bucket("datalake-jg")

with open('/home/hadoop/SCHEMA/JWR_Withdrawals_Str.json', 'r') as f:
        json_df = json.load(f)
schema = StructType.fromJson(json_df)

def getSqlContextInstance(sparkContext):
        if ('sqlContextSingletonInstance' not in globals()):
                globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
        return globals()['sqlContextSingletonInstance']
def f(x):
	d = {}  
        for i in range(len(x)):
                d[str(i)] = x[i]
        return d
def processRDDs(time,rdd):
        print("========= %s =========" % str(time))
        if rdd.count() > 0:  #RDD is Not Empty
                        sqlContext = getSqlContextInstance(rdd.context)
			usersDF = sqlContext.createDataFrame(rdd, schema)
			usersDF.write.mode("append").parquet("s3a://datalake-jg/jwr/FULL_LOAD/WITHDRAWALS/")
	else:
		print("*****************************EMPTY RDD ********************************")
			

if __name__ == "__main__":
        sc = SparkContext(appName="JWR_WITHDRWALS")
        ssc = StreamingContext(sc, 90)
        print("spark context set")
        spark = SparkSession(sc)
        sqlContext = SQLContext(sc)
        zkQuorum, topic = '172.31.4.30:2181','JWR_WITHDRWALS'
        kvs = KafkaUtils.createStream(ssc, zkQuorum, "JWR_WITHDRWALS_group1", {topic: 4}, {"auto.offset.reset" : "smallest"})
        print("********************************************connection set****************************************************************")
        dstream = kvs.map(lambda x: json.loads(x[1]))
        #print("PRINTING DSTREAM .................................")
        #dstream.pprint()
        dstream.foreachRDD(processRDDs) 
        ssc.start()
        ssc.awaitTermination()

