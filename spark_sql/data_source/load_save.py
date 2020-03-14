#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# Manually Specifying Options
local_spark_example_dir = "file://{}/".format(SPARK_HOME)
df = spark.read.load(local_spark_example_dir + "users.parquet")
# default save to hdfs /usr/root下面
df.select("name", "favorite_color").write.save("/test/nameAndFavColors.parquet")
# df = spark.read.load(local_spark_example_dir+"examples/src/main/resources/people.json", format="json")
# df.select("name", "age").write.save("/test/namesAndAges.parquet", format="parquet")

# load csv
df = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';',
                     inferSchema="true", header="true")
# ds = spark.read.csv(local_spark_example_dir + "people.csv", sep=';', header=True)

# run sql on files directly
df = spark.sql(
    "SELECT * FROM parquet.`file:///root/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/users.parquet`")
df.show(2)

# Bucketing, Sorting and Partitioning
df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

df = spark.read.parquet("file:///root/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/users.parquet")
(df
    .write
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .saveAsTable("people_partitioned_bucketed"))