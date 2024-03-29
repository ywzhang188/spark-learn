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

# alias for field, new column
ds.select("A", "B", (ds["C"] * 2).alias("double_c")).show()

# alias, df columns=['id', 'A', 'B'], cols = ['A', 'B'], return ['id', 'A_o', 'B_o']
df.select(*['id']+[((df[c] < bounds[c][0]) | (df[c]>bounds[c][1])).alias(c+'_o') for c in cols])

# create dataframe from Dict
import numpy as np

d = {'A': [0, 1, 0],
     'B': [1, 0, 1],
     'C': [1, 0, 0]}
spark.createDataFrame(np.array(list(d.values())).T.tolist(), list(d.keys())).show()

# create dataframe with Row
from pyspark.sql import Row
row = Row("col1", "col2")
x = ['A', 'B']
y = [1, 2]
new_df = sc.parallelize([row(x[i], y[i]) for i in range(2)]).toDF()

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
ds.first  # 获取第一行记录
ds.head(n)  # 获取前n行记录
ds.take(n)  # 获取前n行数据
ds.takeAsList(n)  # 获取前n行数据，并以List的形式展现

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

df = df.withColumn('testColumn', F.lit('this is a test'))  # add column with constant value

# merge columns into array, multiple columns into list
columns = [F.col("frequency"), F.col("recency")]
intent_score_carbon = df.withColumn("features", F.array(columns))

# transpose row into column 转置, 多行转多列，透视，转换成宽数据
df = spark.createDataFrame([(15,399,2), (15,1401,5), (15,1608,4),
                            (15,20,4), (18,100,3), (18,1401,3)], ["userID","movieID","rating"])
df.groupBy("userID").pivot("movieID").agg(F.first(F.col("rating"))).show()
pivot_df = df.groupBy('userID').pivot('movieID', ['20', '100']).agg(F.sum('rating')).fillna(0)  # 部分透视
# 反透视, 多列转多行， unpivot, 转换成长数据
def to_long(df, by, key, val):

    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = F.explode(F.array([F.struct(F.lit(c).alias(key), F.col(c).alias(val)) for c in cols])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.{}".format(key), "kvs.{}".format(val)]).filter(~F.isnull(val))

to_long(pivot_df, ["userID"], 'movieID', 'rating').show()


# drop duplicates
dropped_df = df.dropDuplicates(subset=['AudienceId'])
display(dropped_df)
# 取交集、差集、并集
df.select('jymc').dropDuplicates().intersect(df.select('jydfmc').dropDuplicates())  # 交集
df.select('jymc').dropDuplicates().subtract(df.select('jydfmc').dropDuplicates())  # 差集
df.select('jymc').dropDuplicates().union(df.select('jydfmc').dropDuplicates())  # 并集
df.select('jymc').union(df.select('jydfmc')).distinct()  # 并集+去重
newDF = df.select("sentence").subtract(df2.select("sentence"))

# timestamp difference seconds
(df.groupBy('sid').agg(F.countDistinct("page_visit_id").alias('count_unique_pv'),
                      F.min("start_time").alias("sid_start_time"),
                      F.max("end_time").alias("sid_end_time"))\
    .withColumn("DiffInSeconds", F.col("sid_end_time").cast("long") - F.col("sid_start_time").cast("long")))


# filter
ds[ds["B"] > 2].show()
ds[(ds['B'] > 2) & (ds['C'] < 6)].show(4)
ds.filter("B>2").filter("C<6").show()
ds.filter((ds.B > 2) & (ds.C < 6)).show()  # can not use "and"
ds.filter((ds["B"] > 2) & (ds["C"] < 6)).show()  # can not use "and"
ds = ds.filter(ds.winner.like('Nat%'))
df = df.filter(df.gameWinner.isin('Cubs', 'Indians'))
df.select(df.name, df.age.between(2, 4).alias("if_between_2_and_4")).show()  # 此处between返回的是True/False, 增加新列，并未筛选
df.select(df.ip.endswith('0').alias('endswithZero')).show(10)
df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()
# rlike
expr = r"Arizona.*hot"
dk = df.filter(df["keyword"].rlike(expr))
df.filter("col2 not like 'MSL%' and col2 not like 'HCP%'")

expr = r"(?i).*archant.*"
df = df.filter(df["script_id"].rlike(expr))
# contains
df.filter(F.col("long_text").contains(F.col("number")))
# where
df.where(F.col('col1').like("%string%"))
df.where((F.col("foo") > 0) | (F.col("bar") < 0))
df.where("attr_value = 35 or income = 99")

# Null 缺失值
df.where(col("dt_mvmt").isNull())
df.where(col("dt_mvmt").isNotNull())

# nested, struct field
df.where(df["x.y.z"] != df["v"])

# for loop in filter
from functools import reduce
from operator import and_
bounds = {'csf_tmsale_1m_pctatm': [-265.90000000000003, 507.70000000000005],
            'csf_tmsale_2m_pctatm': [-395.00000000000006, 811.4000000000001], 'csf_tmsale_3m_pctatm': [-192.5, 575.5]}
outlier_expr  = reduce(and_, [F.col(c) < bounds[c][1] for c in bounds])
new_df = df.where(outlier_expr)

# count na 统计缺失值
df.select(*(F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns))

# Na, missing value
df.where(df.col1.isNotNull()).show()
df.filter(~F.isnull("col1"))
df.na.drop(subset=["col1", "col2", "col3", "col4"], thresh=2)  # Drop row if it does not have at least two values that are **not** NaN
df = df.dropna(thresh=len(df.columns)-2)  # drop rows with 2 or more null values.
df.where(F.col("col1").isNotNull())
df[df['col1'].isNotNull()]  # df.where(df.income.isNull())
# fill na
df.fillna(0, subset=['col1', 'col2'])
df.na.fill('wz', subset=['col1', 'col2'])

df.fillna({'a': 0, 'b': 0})
# fill with mean
mean_val = df.select(F.mean(df['col1'])).collect()
mean_sales = mean_val[0][0]  # to show the number
df.na.fill(mean_sales, subset=['col1'])

# 抽样
t1 = df.sample(False, 0.2, 42)  # 其中withReplacement = True or False代表是否有放回。42是seed

# collect column values to list
df.select(F.collect_list("module_num")).collect()[0][0]
# df.agg(F.collect_list(F.col("item_third_cate_cd"))).collect()[0][0]


# sort
df.sort(F.col('col1').desc())

# calculate column percentile
df.selectExpr('percentile(col1, 0.95)').show()
df.approxQuantile(["income", "expenses", "user_id"], [0.5], 0.25)

# groupby percentile
from pyspark.sql import Window
import pyspark.sql.functions as F
# method 0
grp_window = Window.partitionBy('col1')
magic_percentile = F.expr('percentile_approx(col2, 0.5)')
# magic_percentile = F.expr('percentile_approx(col2, array(0.25, 0.5, 0.75))')
df.withColumn('med_col2', magic_percentile.over(grp_window))
# method 1
df.groupBy('col1').agg(magic_percentile.alias('med_col2'))



# 从StructField中取出嵌套的Row中的值
from pyspark.sql import Row

df = sc.parallelize([Row(col1=Row(a=1, b="b"))]).toDF()
df.select(df.col1.getField("b")).show()
df.select(df.col1.a).show()

# data type
df.select(df.age.cast("string").alias('ages')).collect()
df.select(df.age.cast(StringType()).alias('ages')).collect()

for i in numeric_features:
    new_df = new_df.withColumn(i+'_buckets', F.col(i+'_buckets').cast(IntegerType()).cast(StringType()))


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

# selectExpr
from pyspark.sql.types import *
bucketing = udf(lambda x: 0 if x > 50 else 1, IntegerType())
spark.udf.register("bucketing", bucketing)
df.selectExpr("user_id", "income as new_income", "bucketing(expenses)" ).show()

# time
from pyspark.sql.functions import month, year, dayofmonth, dayofweek, dayofyear

df.withColumn('year', year('jysj')). \
    withColumn('month', month('jysj')). \
    withColumn('day', dayofmonth('jysj')). \
    withColumn('week', dayofweek('jysj')). \
    withColumn('day_num', dayofyear('jysj'))  # 获取对应的年，月，日，一周内第几天，一年内第几天
# date from timestamp
df = df.withColumn("date_only", F.to_date(F.col("DateTime")))


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
# 差集，去除df_top
df = df.join(df_top, ["user_log_acct"], "leftanti")

df.join(df_top, ["user_log_acct"], "leftouter").where(df_top["pick"].isNull()).drop(df_top["pick"]))


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
ds.groupBy('A').agg(F.min('B'), F.max('C'))
ds.groupBy("year").agg(F.countDistinct("id"))

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

# row number window

F.row_number().over(
    Window.partitionBy("col1").orderBy(F.col("unit_count").desc())
)
df.withColumn("row_num", F.row_number().over(Window.partitionBy("col2").orderBy("Date"))-1) # start from 0

# add index column
from pyspark.sql.window import Window
w = Window.orderBy("myColumn")
withIndexDF = df.withColumn("index", F.row_number().over(w))
# method2
df.select('key', 'value1', 'value2', F.row_number().over(
    Window.partitionBy("key").orderBy(F.col("value1").desc())
).alias("index")).show()  # add index col

# inde 索引
df = df.withColumn("id", F.row_number().over(Window.orderBy(F.lit('A'))))

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
# explode array of struct
df.select('pid', F.explode('synced_partners').alias('partners')).select("partners.*", "pid")
# multiple columns into one
df1 = (df.withColumn("word", F.struct("col1", "col2").alias("new_col")))
# groupby word_counts
df2 = (df.withColumn("word", F.explode("col3")) \
       .groupBy("col2", "word").count() \
       .groupBy("col2") \
       .agg(F.collect_list(F.struct("word", "count")).alias("word_counts")))

# groupBy agg multiple columns
expr_mean = [F.avg(col).alias(col+'_mean') for col in numeric_cols]
df_mean = df.groupBy('cluster').agg(F.count(F.lit(1)).alias("audience_num"), *expr_mean).toPandas()

# 计算字符串格式的时间差(时间、日期相关)
df = df.withColumn("days", F.datediff(F.col('ord_dt').cast('date'), F.col('add_dt').cast('date')))  # 计算天数时间差

# 本月最后一天
df = spark.createDataFrame([('1997-02-10',)], ['d'])
df.select(F.last_day(df.d).alias('date')).show()

# 字符串转为时间
test_df = add_df.withColumn("add_dt", F.col("add_dt").cast("date"))
# 时间转回字符串
add_df = add_df.withColumn("window_end", F.date_format('window_end', "yyyy-MM-dd"))


# 时间窗口，移动平均
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#function to calculate number of seconds from number of days
days = lambda i: i * 86400
df = spark.createDataFrame([(17, "2017-03-10"),
                            (15, "2017-03-10"),
                            (3, "2017-03-11"),
                            (13, "2017-03-12"),
                            (19, "2017-03-13"),
                            (25, "2017-03-14"),
                            (22, "2017-03-16")], ["dollars", "timestampGMT"])
df = df.withColumn('timestampGMT', df.timestampGMT.cast('timestamp'))
days = lambda i: i * 86400
#create window by casting timestamp to long (number of seconds)
# w = (Window.orderBy(F.col("add_dt_timestamp").cast('long')).rangeBetween(-days(3), 0)) # 此处-days(3)。注意这里往前看三天不包含当天，其实是4天
w = (Window.orderBy(F.col("timestampGMT").cast('long')).rangeBetween(0, days(2))) # 此处包含当天时间，例如：03-10往后两天到03-12, 其实是一共看了3天
df = df.withColumn('rolling_average', F.avg("dollars").over(w))
# 其实还可以先按天汇总，然后rowsBetween

# group by time period, 时间段分组
interval = 60*60*24*3  # 3天
df.withColumn('time_interval', F.from_unixtime(F.floor(F.unix_timestamp(F.col('timestampGMT'), "yyyy-MM-dd") / interval) * interval)) \
    .groupBy('time_interval') \
    .agg(F.collect_list("id").alias("id_list")).show()

# group by time period, 时间段分组2
df = df.withColumn("date_index", F.dense_rank().over(Window.orderBy(F.col('timestampGMT'))))
date_bucket_func = udf(lambda x: x//3 if x%3 else x//3-1)
df = df.withColumn("date_bucket", date_bucket_func(F.col('date_index')))