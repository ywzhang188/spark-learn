#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

peopleDF = spark.read.json(local_spark_example_dir+"examples/src/main/resources/people.json")

# DataFrames can be saved as Parquet files, maintaining the schema information
peopleDF.write.parquet("/test/people.parquet")
parquetFile = spark.read.parquet("/test/people.parquet")
# parquet files can also be used to create a temporary view and then used in SQL statements
parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <=19")
teenagers.show()

# Schema Merging
from pyspark.sql import Row
sc = spark.sparkContext

squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, double=i ** 2)))
squaresDF.write.parquet("/test/test_table/key=1")

# Create another DataFrame in a new partition directory
# adding a new column and dropping an existing column
cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11)).map(lambda i: Row(single=i, triple=i**3)))
cubesDF.write.parquet("/test/test_table/key=2")
# read the partitioned table
mergedDF = spark.read.option("mergeSchema", "true").parquet("/test/test_table")
mergedDF.printSchema()