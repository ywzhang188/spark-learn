#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# Manually Specifying Options
local_spark_example_dir = "file://{}/examples/src/main/resources/".format(SPARK_HOME)
df = spark.read.load(local_spark_example_dir + "users.parquet")
# default save to hdfs /usr/root下面
df.select("name", "favorite_color").write.save("/test/nameAndFavColors.parquet")
# df = spark.read.load(local_spark_example_dir+"examples/src/main/resources/people.json", format="json")
# df.select("name", "age").write.save("/test/namesAndAges.parquet", format="parquet")

# load csv
df = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';',
                     inferSchema="true", header="true")
ds = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(
    local_spark_example_dir + "people.csv")
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

# 写出在一个csv 文件中
path = "s3://datascience-p-euwest1-carbon/Yuwei/word_interest_similarity"
df.repartition(1).write.csv(path=path, header=True, sep=",", mode='overwrite', encoding='utf-8')
# 多个csv
df.write.csv(path=path, header=True, sep=",", mode='overwrite')
# 保存为parquet格式数据。读取速度快，占用内存小
df.select("col1", "col2").write.save("test.parquet")  # 实测
df.write.parquet(path=path, mode='overwrite')
