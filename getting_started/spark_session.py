#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

import os
from pyspark.sql import SparkSession

SPARK_HOME = '/root/apps/spark-2.4.4-bin-hadoop2.7'
os.environ['SPARK_HOME'] = SPARK_HOME
os.environ['JAVA_HOME'] = '/root/apps/jdk1.8.0_221'
os.environ["PYSPARK_PYTHON"]="/root/python_envs/spark_p37/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/root/python_envs/spark_p37/bin/python3"
local_spark_example_dir = "file://{}/".format("/root/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources")

# Starting Point: SparkSession
spark = SparkSession.builder.appName("Read Parquet Data").config("spark.some.config.option", "some-value").getOrCreate()

sc = spark.sparkContext


#
spark = (SparkSession
         .builder
         .appName("test-dockerlinuxcontainer")
         .enableHiveSupport()
         .config("spark.executor.instances", "50")
         .config("spark.executor.memory","48g")
         .config("spark.executor.cores","24")
         .config("spark.driver.memory","48g")
         .config("spark.sql.shuffle.partitions","500")
         .config("spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class","DockerLinuxContainer")
         .config("spark.executorEnv.yarn.nodemanager.container-executor.class","DockerLinuxContainer")
         .config("spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name","bdp-docker.jd.com:5000/wise_mart_bag:latest")
         .config("spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name","bdp-docker.jd.com:5000/wise_mart_bag:latest")
         .getOrCreate())
