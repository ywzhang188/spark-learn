#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

os.environ['SPARK_HOME'] = '/root/apps/spark-2.4.4-bin-hadoop2.7'
os.environ['JAVA_HOME'] = '/root/apps/jdk1.8.0_221'
os.environ['HADOOP_HOME'] = "/root/apps/hadoop-3.1.2"
sys.path.append('/root/apps/spark-2.4.4-bin-hadoop2.7/python/lib')
sys.path.append('/root/apps/spark-2.4.4-bin-hadoop2.7/python')


conf = SparkConf().setAppName("read_parquet").setMaster("spark://bigdata1:7077").setExecutorEnv("spark.executor.memory", "800m")
sc = SparkContext(conf=conf)

# sc = SparkContext(master="spark://bigdata1:7077",appName="read_parquet")

# conf = SparkConf("spark://bigdata1:7077").setAppName("read_parquet")
# sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

# df = sqlContext.read.parquet("file:///root/data-sample-for-glam.snappy.parquet")
df = sqlContext.read.parquet("hdfs://bigdata1:9000/carbon_data/sample_data.parquet")
df.registerTempTable("carbon_data")
sqlContext.sql("select locations from carbon_data").show(10, truncate=False)

