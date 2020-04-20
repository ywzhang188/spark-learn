#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# create dataframe from list
my_list = [['a', 1, 2], ['b', 2, 3], ['c', 3, 4]]
col_name = ['A', 'B', 'C']
ds = spark.createDataFrame(my_list, col_name)

ds[ds['A'], ds['B']].show()
ds.select(ds.A, ds.B).show()
ds.select("A", "B").show()

# alias for field
ds.select("A", "B", (ds["C"] * 2).alias("double_c")).show()

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

# add columns
import pyspark.sql.functions as F

df = df_with_winner.withColumn('testColumn', F.lit('this is a test'))

# drop duplicates
dropped_df = df.dropDuplicates(subset=['AudienceId'])
display(dropped_df)

# filter
ds[ds["B"] > 2].show()
ds[(ds['B'] > 2) & (ds['C'] < 6)].show(4)
ds.filter("B>2").filter("C<6").show()
ds.filter((ds.B > 2) & (ds.C < 6)).show()  # can not use "and"
ds.filter((ds["B"] > 2) & (ds["C"] < 6)).show()  # can not use "and"
ds = ds.filter(ds.winner.like('Nat%'))
df = df.filter(df.gameWinner.isin('Cubs', 'Indians'))
df.select(df.name, df.age.between(2, 4)).show()
df.select(df.ip.endswith('0').alias('endswithZero')).show(10)
df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()

# sort
df.sort(F.col('col1').desc())

# 从StructField中取出嵌套的Row中的值
from pyspark.sql import Row

df = sc.parallelize([Row(r=Row(a=1, b="b"))]).toDF()
df.select(df.r.getField("b")).show()
df.select(df.r.a).show()

# data type
df.select(df.age.cast("string").alias('ages')).collect()
df.select(df.age.cast(StringType()).alias('ages')).collect()

# 如果列中的值为list或dict,则根据index或key取相应的值
df = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])
df.select(df.l.getItem(0).alias('first of l'), df.d.getItem("key").alias('value of d')).show()
df.select(df.l[0], df.d["key"]).show()

# order
ds.select("A", "B").orderBy("C", ascending=False).show()
ds.select("A", "B").orderBy(ds.C.desc()).show()
# multi fields
ds.select("A").orderBy(["B", "C"], ascending=[0, 1])
ds.orderBy(ds.B.desc(), ds.A).show()

# distinct
ds.select('A').distinct().show()
ds.select('A', 'B').distinct().show()

# with new column
import pyspark.sql.functions as F

df.withColumn("first_two", F.array([F.col("letters")[0], F.col("letters")[1]])).show()

ds.withColumn('D', F.log(ds.C)).show(4)
ds.withColumn('F10', ds.C + 10).show(4)

ds.withColumn('D', ds['C'] / ds.groupBy().agg(F.sum("C")).collect()[0][0]).show(4)

ds.withColumn('cond', F.when((ds.B > 1) & (ds.C < 5), 1).when(ds.A == 'male', 2).otherwise(3)).show(4)

ds = ds.withColumn('new_column',
                   F.when(F.col('col1') > F.col('col2'), F.col('col1')).otherwise('other_value'))

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

maturity_udf = udf(lambda col1: "adult" if col1 > 1 else "child", StringType())

df.withColumn("maturity", maturity_udf(df.col1)).show()


def generate_udf(constant_var):
    def test(col1, col2):
        if col1 == col2:
            return col1
        else:
            return constant_var

    return F.udf(test, StringType())


ds = ds.withColumn('new_column',
                   generate_udf('default_value')(F.col('col1'), F.col('col2')))


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
lefts.join(rights, on='A', how='left').orderBy('A', ascending=True).show()
lefts.join(rights, lefts["A"] == rights["A"], "left").show()
# right join
lefts.join(rights, on='A', how='right').orderBy('A', ascending=True).show()
# inner join
lefts.join(rights, on='A', how='inner').orderBy('A', ascending=True).show()
# full join
lefts.join(rights, on='A', how='full').orderBy('A', ascending=True).show()

# concat columns(合并字符串)
my_list = [('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9),
           ('a', 2, 3),
           ('b', 5, 6),
           ('c', 8, 9)]
col_name = ['col1', 'col2', 'col3']
ds = spark.createDataFrame(my_list, schema=col_name)
ds.withColumn('concat', F.concat('col1', 'col2')).show()
ds.withColumn('concat', F.concat('col1', F.lit(' vs '), 'col2')).show()

# GroupBy
ds.select("col1").groupBy("col1").count().sort(F.col('count').desc())
ds.groupBy("col1").count().show()
ds.groupBy("col1", "col2").count().orderBy("col1", "col2").show()
ds.groupBy(['col1']).agg({'col2': 'min', 'col3': 'avg'}).show()

# crosstab
ds.stat.crosstab("col1", "col3").show()

# pivot
ds.groupBy(['col1']).pivot('col2').sum('col3').show()

# Window
d = {'A': ['a', 'b', 'c', 'd'], 'B': ['m', 'm', 'n', 'n'], 'C': [1, 2, 3, 6]}
dp = pd.DataFrame(d)
ds = spark.createDataFrame(dp)

from pyspark.sql.window import Window

w = Window.partitionBy('B').orderBy(ds.C.desc())
ds = ds.withColumn('rank', F.rank().over(w))

# rank vs dense_rank
d = {'Id': [1, 2, 3, 4, 5, 6],
     'Score': [4.00, 4.00, 3.85, 3.65, 3.65, 3.50]}
data = pd.DataFrame(d)
ds = spark.createDataFrame(data)
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w = Window.orderBy(ds.Score.desc())
ds = ds.withColumn('Rank_spark_dense', F.dense_rank().over(w))
ds = ds.withColumn('Rank_spark', F.rank().over(w))
ds.show()

# 统计描述
ds.describe().show()

# retype column
rawDataWithHeader = sc.textFile('/test/train.tsv')
r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
header = r_lines.first()
lines = r_lines.filter(lambda x: x != header)
ds_raw = spark.createDataFrame(lines, header)
# method 1
from pyspark.sql.functions import col

ds = ds_raw.select(
    [ds_raw.columns[3]] + [col(column).cast("float") for column in ds_raw.columns[4:-1]] + [ds_raw.columns[-1]])
# method 2
from pyspark.sql.types import FloatType
for column in ds.columns[4:-1]:
    ds = ds.withColumn(column, ds_raw[column].cast(FloatType()))
# method 3
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

to_float = udf(lambda x: "0" if x == "?" else x, StringType())
from pyspark.sql.functions import col

ds = ds_raw.select(
    [ds_raw.columns[3]] + [to_float(col(column)).cast("float").alias(column) for column in ds_raw.columns[4:-1]] + [
        ds_raw.columns[-1]])

# machine learning process
ds_raw = spark.read.load("/test/train.tsv", format='csv', sep='\t', inferSchema="true", header="true")
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

to_float = udf(lambda x: 0 if x == "?" else float(x), FloatType())
from pyspark.sql.functions import col

ds = ds_raw.select([ds_raw.columns[3]] + [to_float(col(column)).alias(column) for column in ds_raw.columns[4:]])

from pyspark.ml.feature import StringIndexer

categoryIndexer = StringIndexer(inputCol="alchemy_category", outputCol="alchemy_category_index")
categoryTransformer = categoryIndexer.fit(ds)
df1 = categoryTransformer.transform(ds)
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(dropLast=False, inputCol="alchemy_category_index", outputCol="alchemy_category_index_vector")
df2 = encoder.transform(df1)
from pyspark.ml.feature import VectorAssembler

assemblerInput = ['alchemy_category_index_vector'] + ds.columns[1:-1]
assembler = VectorAssembler(inputCols=assemblerInput, outputCol="features")
df3 = assembler.transform(df2)
# deal with categorical label
from pyspark.ml.feature import StringIndexer

# Index labels, adding metadata to the label column
labelIndexer = StringIndexer(inputCol='label',
                             outputCol='indexedLabel').fit(df3)
df4 = labelIndexer.transform(df3)
from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="features", impurity="gini", maxDepth=10, maxBins=14)
dt_model = dt.fit(df4)
df5 = dt_model.transform(df4)
# Convert indexed labels back to original labels.
from pyspark.ml.feature import IndexToString

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
df6 = labelConverter.transform(df5)
df6.crosstab("label", "predictedLabel").show()
# pipeline
from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[categoryIndexer, encoder, assembler, labelIndexer, dt, labelConverter])
pipeline.getStages()
pipelineModel = pipeline.fit(ds)
pipelineModel.stages[-2].toDebugString
predicted = pipelineModel.transform(ds)
predicted.crosstab("label", "predictedLabel").show()

# explode array into row
df = spark.createDataFrame([(1, "A", [1, 2, 3]), (2, "B", [3, 5]), (8, "B", [3, 6])], ["col1", "col2", "col3"])
df.withColumn("col3", F.explode(df.col3)).show()
# groupby word_counts
df2 = (df.withColumn("word", F.explode("col3")) \
       .groupBy("col2", "word").count() \
       .groupBy("col2") \
       .agg(F.collect_list(F.struct("word", "count")).alias("word_counts")))
