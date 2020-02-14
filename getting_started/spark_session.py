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

# Untyped Dataset Operations (aka DataFrame Operations)
df = spark.read.parquet("/carbon_data/sample_data.parquet")
df.printSchema()
df.dtypes
df.select('locations.country', 'synced').show(5, truncate=False)
df.groupby("devices.os").count().show()
df.groupby("locations.country", "devices.os").count().show()
# Register the DataFrame as a SQL temporary view, Running SQL Queries Programmatically
df.createOrReplaceTempView("carbon_sample")
sqlDF = spark.sql("select * from carbon_sample limit 1")
sqlDF.show(1, truncate=False)

# interoperating with RDDs
from pyspark.sql import Row
sc = spark.sparkContext
# load a text file and convert each line to a Row
lines = sc.textFile("file://{}".format(os.path.join(SPARK_HOME, "examples/src/main/resources/people.txt")))
parts = lines.map(lambda l: l.split(","))
# each line is converted to a tuple
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
# Infer the schema, and register the DataFrame as a table
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")
# The results of SQL queries are Dataframe objects.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
teenagers.show()
teenagers.toPandas()
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: "+p.name).collect()
# teenNames is a list
for name in teenNames:
    print(name)

# programmatically specifying the schema
from pyspark.sql.types import *
sc = spark.sparkContext
# Load a text file and convert each line to a Row,从原始RDD创建元组
lines = sc.textFile("file://{}".format(os.path.join(SPARK_HOME, "examples/src/main/resources/people.txt")))
parts = lines.map(lambda l: l.split(","))
# converted to a row
people = parts.map(lambda p: (p[0], p[1].strip()))
# the schema is encoded in a string创建一个由StructType表示的模式来匹配元组或列表的结构
schemaString = "name age"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
# apply
schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.createOrReplaceTempView("people")
results = spark.sql("SELECT name FROM people")
results.show()
