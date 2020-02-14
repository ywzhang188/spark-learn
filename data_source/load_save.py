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

# Starting Point: SparkSession
spark = SparkSession.builder.appName("Read Parquet Data").getOrCreate()