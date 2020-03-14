#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *
import numpy as np
import pandas as pd

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

df2 = spark.sparkContext.parallelize([(1,2,3, 'a b c'), (4,5,6,'def')])
df2.toDF(['col1', 'col2', 'col3', 'col4']).show()

df3 = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

# From Dict
d = {'A': [0, 1, 0],
     "B": [1, 0, 1],
     'C': [1, 0, 0]}
spark.createDataFrame(np.array(list(d.values())).T.tolist(), list(d.keys()))

my_list = [['male', 1, None], ['female', 2, 3], ['male', 3, 4]]
ds = spark.createDataFrame(my_list, ['A', 'B', 'C'])
# fillna
ds.fillna(-99).show()
# replace value
ds.replace(['male', 'female'], ['1', '0']).show()
# rename columns
ds.toDF('a', 'b', 'c').show(4)
# rename one or more columns
mapping = {'A': "sex", "B": 'sales'}
new_names = [mapping.get(col, col) for col in ds.columns]
ds.toDF(*new_names).show()
# rename one column
ds.withColumnRenamed("A", "Paper").show(4)
# drop columns
drop_name = ['A', 'B']
ds.drop(*drop_name).show(4)
# filter
ds = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';', inferSchema="true", header="true")
ds[(ds["age"]>30)].show()
# with new column
from pyspark.sql import functions as F
# todo
ds.withColumn('tv_norm', ds.age/ds.groupby('name').agg(F.sum("age"))).show()
ds.withColumn('cond',F.when((ds.age<31),1).when(ds.age>30, 2).otherwise(3)).show(4)
ds.withColumn('log_age', F.log(ds.age)).show()
ds.withColumn('age+10', ds.age+10).show()

# join dataframe
leftp = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                      'B': ['B0', 'B1', 'B2', 'B3'],
                      'C': ['C0', 'C1', 'C2', 'C3'],
                      'D': ['D0', 'D1', 'D2', 'D3']},
                     index=[0, 1, 2, 3])
rightp = pd.DataFrame({'A': ['A0', 'A1', 'A6', 'A7'],
                       'F': ['B4', 'B5', 'B6', 'B7'],
                       'G': ['C4', 'C5', 'C6', 'C7'],
                       'H': ['D4', 'D5', 'D6', 'D7']},
                      index=[4, 5, 6, 7])
lefts = spark.createDataFrame(leftp)
rights = spark.createDataFrame(rightp)
lefts.join(rights, on='A', how='left').orderBy('A', ascending=True).show()
lefts.join(rights, on='A', how='right').orderBy('A', ascending=True).show()
lefts.join(rights,on='A', how='inner').orderBy('A', ascending=True).show()
lefts.join(rights, on='A', how='full').orderBy('A', ascending=True).show()

# concat columns(字符串拼接)
my_list = [('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9),
           ('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9)
           ]
col_name = ['col1', 'col2', 'col3']
ds = spark.createDataFrame(my_list, schema=col_name)
ds.withColumn('concat', F.concat('col1', 'col2')).show()

# groupBy
ds.groupby(['col1']).agg({'col2':'min', 'col3':'avg'}).show()

# pivot
ds.groupby(['col1']).pivot('col2').sum('col3').show()
ds.groupby(['col1']).pivot('col2').agg({'col3':'sum'}).show()

# Window
d = {'A':['a','b','c','d'],'B':['m','m','n','n'],'C':[1,2,3,6]}
dp = pd.DataFrame(d)
ds = spark.createDataFrame(dp)

from pyspark.sql.window import Window
w = Window.partitionBy("B").orderBy(ds.C.desc())
ds = ds.withColumn('rank', F.rank().over(w))
ds = ds.withColumn('rank', F.dense_rank().over(w))

# rank vs dense_rank
d = {'Id':[1,2,3,4,5,6],
     'Score': [4.00, 4.00, 3.85, 3.65, 3.65, 3.50]}
data = pd.DataFrame(d)
dp = data.copy()
ds = spark.createDataFrame(data)
# pandas
dp['Rank_dense'] = dp['Score'].rank(method='dense',ascending =False)
dp['Rank'] = dp['Score'].rank(method='min',ascending =False)
print(dp)
# pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
w = Window.orderBy(ds.Score.desc())
ds = ds.withColumn('Rank_spark_dense',F.dense_rank().over(w))
ds = ds.withColumn('Rank_spark',F.rank().over(w))
ds.show()


