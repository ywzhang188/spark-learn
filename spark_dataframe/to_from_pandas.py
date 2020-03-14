#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# enabling for conversion to/from Pandas
# toPandas()将Spark DataFrame 转换为 Pandas DataFrame
# createDataFrame(pandas_df)从Pandas DataFrame 创建 Spark DataFrame
import numpy as np
import pandas as pd

# enable arrow-based columnar data transfer
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Generate a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))
# create s spark dataframe from a pandas dataframe
df = spark.createDataFrame(pdf)
# convert the spark dataframe back to a pandas dataframe using arrow
result_df = df.select("*").toPandas()

# pandas UDFS
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType


# Declare the function and create the UDF
def multiply_func(a, b):
    return a * b


multiply = pandas_udf(multiply_func, returnType=LongType())
# the function for a pandas_udf should be able to execute with local Pandas data
x = pd.Series([1, 2, 3])
print(multiply_func(x, x))
# create a spark DataFrame, 'spark' is an existing  SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=['x']))
# execute function as a Spark vectorized UDF
df.select(multiply(col("x"), col("x"))).show()

# grouped Map
from pyspark.sql.functions import pandas_udf, PandasUDFType

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))


@pandas_udf("id long, v double", PandasUDFType.GROUPED_MAP)
def substarct_mean(pdf):
    v = pdf.v
    return pdf.assign(v=v - v.mean())


df.groupby("id").apply(substarct_mean).show()

# rdd.DataFrame vs pd.DataFrame
my_list = [['a', 1, 2], ['b', 2, 3], ['c', 3, 4]]
col_name = ['A', 'B', 'C']
pd.DataFrame(my_list, columns=col_name)
ds = spark.createDataFrame(my_list, col_name)
