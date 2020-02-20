#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

# create dataframe from list
my_list = [['a', 1, 2], ['b', 2, 3], ['c', 3, 4]]
col_name = ['A', 'B', 'C']
spark.createDataFrame(my_list, col_name).show()

# create dataframe from Dict
import numpy as np

d = {'A': [0, 1, 0],
     'B': [1, 0, 1],
     'C': [1, 0, 0]}
spark.createDataFrame(np.array(list(d.values())).T.tolist(), list(d.keys())).show()

# load datafram from database
# connect to database
host = '##.###.###.##'
db_name = 'db_name'
user = ""
pw = ""
url = 'jdbc:postgresql://' + host + ':5432/' + db_name + '?user=' + user + '&password=' + pw
properties = {'driver': 'org.postgresql.Driver', 'password': pw, 'user': user}
table_name = ""
ds = spark.read.jdbc(url=url, table=table_name, properties=properties)

# load dataframe from .csv
# ds = spark.read.csv(local_spark_example_dir + "people.csv", sep=';', header=True)
ds = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';',
                     inferSchema="true", header="true")

# load dataframe from .json
ds = spark.read.json(local_spark_example_dir + 'people.json')

# first n rows
ds.show(3)

# column names
ds.columns

# data types
ds.dtypes

# fill null
my_list = [['male', 1, None], ['female', 2, 3], ['male', 3, 4]]
ds = spark.createDataFrame(my_list, ['A', 'B', 'C'])
ds.fillna(-99).show()

# replace values
# ds.replace(['male','female'],['1','0']).show()
# caution: you need to chose specific col
ds.A.replace(['male', 'female'], [1, 0], inplace=True)
# caution: Mixed type replacements are not supported
ds.na.replace(['male', 'female'], ['1', '0']).show()

# rename columns
ds.toDF('a', 'b', 'c').show(4)

# rename one or more columns
mapping = {'C': 'c', 'D': 'c'}
new_names = [mapping.get(col, col) for col in ds.columns]
ds.toDF(*new_names).show(4)

# use withColumnRenamed to rename one column in pyspark
ds.withColumnRenamed('C', 'c').show(4)

# drop columns
drop_name = ['A', 'B']
ds.drop(*drop_name).show(4)

# filter
ds[ds["B"] > 2].show()
ds[(ds['B'] > 2) & (ds['C'] < 6)].show(4)

# with new column
import pyspark.sql.functions as F

ds.withColumn('D', F.log(ds.C)).show(4)
ds.withColumn('F10', ds.C + 10).show(4)

ds.withColumn('D', ds['C'] / ds.groupBy().agg(F.sum("C")).collect()[0][0]).show(4)

ds.withColumn('cond', F.when((ds.B > 1) & (ds.C < 5), 1).when(ds.A == 'male', 2).otherwise(3)).show(4)

# join dataframe
import pandas as pd
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

# left join
lefts.join(rights,on='A',how='left').orderBy('A',ascending=True).show()
# right join
lefts.join(rights,on='A',how='right').orderBy('A',ascending=True).show()
# inner join
lefts.join(rights,on='A',how='inner').orderBy('A',ascending=True).show()
# full join
lefts.join(rights,on='A',how='full').orderBy('A',ascending=True).show()

# concat columns
my_list = [('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9),
           ('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9)]
col_name = ['col1', 'col2', 'col3']
ds = spark.createDataFrame(my_list,schema=col_name)
ds.withColumn('concat',F.concat('col1','col2')).show()

# GroupBy
ds.groupBy(['col1']).agg({'col2': 'min', 'col3': 'avg'}).show()

# pivot
ds.groupBy(['col1']).pivot('col2').sum('col3').show()

# Window
d = {'A':['a','b','c','d'],'B':['m','m','n','n'],'C':[1,2,3,6]}
dp = pd.DataFrame(d)
ds = spark.createDataFrame(dp)

from pyspark.sql.window import Window
w = Window.partitionBy('B').orderBy(ds.C.desc())
ds = ds.withColumn('rank',F.rank().over(w))

# rank vs dense_rank
d ={'Id':[1,2,3,4,5,6],
    'Score': [4.00, 4.00, 3.85, 3.65, 3.65, 3.50]}
data = pd.DataFrame(d)
ds = spark.createDataFrame(data)
import pyspark.sql.functions as F
from pyspark.sql.window import Window
w = Window.orderBy(ds.Score.desc())
ds = ds.withColumn('Rank_spark_dense',F.dense_rank().over(w))
ds = ds.withColumn('Rank_spark',F.rank().over(w))
ds.show()

# 统计描述
ds.describe().show()
