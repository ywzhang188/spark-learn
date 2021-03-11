#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='zhangyuwei37'

from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

# local模式下使用hive
appName = "vlt_test"
conf = SparkConf().setAppName(appName).setMaster("local")
sc = SparkContext(conf=conf)
hc = HiveContext(sc)
try:
    data = hc.sql("SELECT * FROM app.app_vdp_ai_sink_prod_rate_among_total")
    data.show(n=2)
finally:
    sc.stop()

# local模式下使用spark sql
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .master("local") \
    .enableHiveSupport() \
    .getOrCreate()
try:
    result = spark.sql("SELECT * FROM app.app_vdp_ai_sink_prod_rate_among_total")
    #result.collect()
    result.show()
finally:
    spark.stop()
print("ends...")