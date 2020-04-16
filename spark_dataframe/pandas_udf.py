#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

# slice from array
df = spark.createDataFrame([([1, 2, 3], 'val1'), ([4, 5, 6], 'val2')], ['col1', 'col2'])
df.show()
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd


# method1
@pandas_udf(ArrayType(LongType()))
def func(v):
    res = []
    for row in v:
        res.append(row[1:])
    return pd.Series(res)


# method 2
@pandas_udf(ArrayType(LongType()))
def func(v):
    return v.apply(lambda x: x[1:])


df.withColumn('col3', func(df.col1)).show()

# create mew column with apply func
df = spark.createDataFrame(
    [('1', 1.0), ('1', 2.0), ('2', 2.0), ('2', 3.0), ('3', 3.0)],
    ("id", "value"))
schema = StructType(
    [StructField('id', StringType()), StructField('value', DoubleType()), StructField('value2', DoubleType())])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def subtract_mean(pdf):
    return pdf.assign(value2=pdf.value - pdf.value.mean())


df.groupby('id').apply(subtract_mean).take(2)
