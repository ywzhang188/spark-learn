#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# load csv
df = spark.read.load("file:///root/datasets/OnlineRetail.csv", format='csv', sep=',',
                     inferSchema="true", header="true")

# check null distribution
from pyspark.sql.functions import count


def my_count(ds):
    ds.agg(*[count(c).alias(c) for c in ds.columns]).show()


my_count(df)

df = df.dropna(how='any')
my_count(df)

# dealwith date
from pyspark.sql.functions import to_utc_timestamp, unix_timestamp, lit, col

timeFmt = "MM/dd/yy HH:mm"
df = df.withColumn('NewInvoiceDate',
                   to_utc_timestamp(unix_timestamp(col('InvoiceDate'), timeFmt).cast('timestamp'), 'utc'))

# calculate total price
from pyspark.sql.functions import round

df = df.withColumn('TotalPrice', round(df.Quantity * df.UnitPrice, 2))

# calculate the time difference
from pyspark.sql.functions import min, max, sum, datediff

date_max = df.select(max('NewInvoiceDate')).toPandas()
current = to_utc_timestamp(unix_timestamp(lit(str(date_max.iloc[0][0])), \
                                          'yy-MM-dd HH:mm').cast('timestamp'), 'UTC')

# Calculatre Duration
df = df.withColumn('Duration', datediff(lit(current), 'NewInvoiceDate'))

# build the Recency, Frequency, Monetary
recency = df.groupBy('CustomerID').agg(min('Duration').alias('Recency'))
frequency = df.groupBy('CustomerID', 'InvoiceNo').count() \
    .groupBy('CustomerID') \
    .agg(count("*").alias("Frequency"))
monetary = df.groupBy('CustomerID').agg(round(sum('TotalPrice'), 2).alias('Monetary'))
rfm = recency.join(frequency, 'CustomerID', how='inner') \
    .join(monetary, 'CustomerID', how='inner')

# rfm segmentation
# determine cutting points
cols = ['Recency', 'Frequency', 'Monetary']
import numpy as np
import pandas as pd


def describe_pd(df_in, columns, deciles=False):
    if deciles:
        percentiles = np.array(range(0, 110, 10))
    else:
        percentiles = [25, 50, 75]
    percs = np.transpose([np.percentile(df_in.select(x).collect(), percentiles) for x in columns])
    percs = pd.DataFrame(percs, columns=columns)
    percs['summary'] = [str(p) + '%' for p in percentiles]
    spark_describe = df_in.describe().toPandas()
    new_df = pd.concat([spark_describe, percs], ignore_index=True)
    new_df = new_df.round(2)
    return new_df[['summary'] + columns]

def describe_dataframe(df):
    df_describe = df.select(*numeric_features).describe().toPandas()
    df_describe_2 = df.approxQuantile(numeric_features, [0.25, 0.5, 0.75], 0.05)
    df_describe.loc[5] = ['25percentile']+[i[0] for i in df_describe_2]
    df_describe.loc[6] = ['median']+[i[1] for i in df_describe_2]
    df_describe.loc[7] = ['75percentile']+[i[2] for i in df_describe_2]
    return df_describe
describe_dataframe(df)


describe_pd(rfm, cols, False)


# cutting points
def RScore(x):
    if x <= 16:
        return 1
    elif x <= 50:
        return 2
    elif x <= 143:
        return 3
    else:
        return 4


def FScore(x):
    if x <= 1:
        return 4
    elif x <= 3:
        return 3
    elif x <= 5:
        return 2
    else:
        return 1


def MScore(x):
    if x <= 293:
        return 4
    elif x <= 648:
        return 3
    elif x <= 1611:
        return 2
    else:
        return 1


from pyspark.sql.types import StringType
import pyspark.sql.functions as F

R_udf = F.udf(lambda x: RScore(x), StringType())
F_udf = F.udf(lambda x: FScore(x), StringType())
M_udf = F.udf(lambda x: MScore(x), StringType())

# RFM Segmentation
rfm_seg = rfm.withColumn("r_seg", R_udf("Recency"))
rfm_seg = rfm_seg.withColumn("f_seg", F_udf("Frequency"))
rfm_seg = rfm_seg.withColumn("m_seg", M_udf("Monetary"))
rfm_seg.show(5)

rfm_seg = rfm_seg.withColumn('RFMScore',
                             F.concat(F.col('r_seg'), F.col('f_seg'), F.col('m_seg')))
rfm_seg.sort(F.col('RFMScore')).show(5)

# statistical summary
simple_summary = rfm_seg.groupby('RFMScore').agg({"Recency": "mean", "Frequency": "mean", "Monetary": "mean"}).sort(
    F.col('RFMScore'))

# Extension: apply k-means clustering section to do the segmentation
from pyspark.ml.linalg import Vectors


def transData(df):
    return df.rdd.map(lambda r: [r[0], Vectors.dense(r[1:])]).toDF(['CustomerID', 'rfm'])


transformed_df = transData(rfm)

# scale the feature matrix
from pyspark.ml.feature import MinMaxScaler

scaler = MinMaxScaler(inputCol='rfm', outputCol="features")
scalerModel = scaler.fit(transformed_df)
scaledData = scalerModel.transform(transformed_df)
scaledData.show(5, False)

# K-means clustering
