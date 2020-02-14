#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *


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