#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

ds = "spark.dataframe"


# Convert to float format
def string_to_float(x):
    return float(x)


#
def condition(r):
    if (0 <= r <= 4):
        label = "low"
    elif (4 < r <= 6):
        label = "medium"
    else:
        label = "high"
    return label


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType

# 将字符串label, 如"1", "2", etc,转为DoubleType
string_to_float_udf = udf(string_to_float, DoubleType())
# j将数值label, 离散化为字符串StringType
quality_udf = udf(lambda x: condition(x), StringType())

ds = ds.withColumn("quality", quality_udf("quality"))
ds.show(5, True)


# get dummy
def get_dummy(df, indexCol, categoricalCols, continuousCols, labelCol, dropLast=False):
    '''
    Get dummy variables and concat with continuous variables for ml modeling.
    :param df: the dataframe
    :param categoricalCols: the name list of the categorical data
    :param continuousCols:  the name list of the numerical data
    :param labelCol:  the name of label column
    :param dropLast:  the flag of drop last column
    :return: feature matrix
    >>> df = spark.createDataFrame([
                  (0, "a"),
                  (1, "b"),
                  (2, "c"),
                  (3, "a"),
                  (4, "a"),
                  (5, "c")
              ], ["id", "category"])
    >>> indexCol = 'id'
    >>> categoricalCols = ['category']
    >>> continuousCols = []
    >>> labelCol = []
    >>> mat = get_dummy(df,indexCol,categoricalCols,continuousCols,labelCol)
    >>> mat.show()
    >>>
        +---+-------------+
        | id|     features|
        +---+-------------+
        |  0|[1.0,0.0,0.0]|
        |  1|[0.0,0.0,1.0]|
        |  2|[0.0,1.0,0.0]|
        |  3|[1.0,0.0,0.0]|
        |  4|[1.0,0.0,0.0]|
        |  5|[0.0,1.0,0.0]|
        +---+-------------+
    '''
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
    from pyspark.sql.functions import col
    indexers = [StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                for c in categoricalCols]
    # default setting: dropLast=True
    encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(),
                              outputCol="{0}_encoded".format(indexer.getOutputCol()), dropLast=dropLast)
                for indexer in indexers]
    assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                                          + continuousCols, outputCol="features")
    pipeline = Pipeline(stages=indexers + encoders + [assembler])
    model = pipeline.fit(df)
    data = model.transform(df)
    if indexCol and labelCol:
        # for supervised learning
        data = data.withColumn('label', col(labelCol))
        return data.select(indexCol, 'features', 'label')
    elif not indexCol and labelCol:
        # for supervised learning
        data = data.withColumn('label', col(labelCol))
        return data.select('features', 'label')
    elif indexCol and not labelCol:
        # for unsupervised learning
        return data.select(indexCol, 'features')
    elif not indexCol and not labelCol:
        # for unsupervised learning
        return data.select('features')


# dealwith the invoiceDate
from pyspark.sql.functions import to_utc_timestamp, unix_timestamp, lit, datediff, col

timeFmt = "MM/dd/yy HH:mm"
ds = ds.withColumn('NewInvoiceDate',
                   to_utc_timestamp(unix_timestamp(col('InvoiceDate'), timeFmt).cast('timestamp'), 'utc'))

# analyze missing values counts
import pyspark.sql.functions as F

df.select([F.count(F.when(F.isnull(c) | F.isnan(c), c)).alias(c) for c in df.columns])

# remove blank string
df = df.filter("colName != ''")

# calculate outlier, drop outlier
cols = numeric_features
bounds = {}
for col in cols:
    quantiles = df.approxQuantile(col,[0.25,0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    bounds[col] = [
        quantiles[0] - 1.5*IQR,
        quantiles[1] + 1.5*IQR
    ]
print(bounds)

outlier_expr = reduce(and_, [F.col(c) < bounds[c][1] for c in outlier_cols])
df = df.where(outlier_expr)

# calculate outlier, 3 sigma, standard deviation
# df.select(
#     [F.mean(c).alias(c+'_mean') for c in outlier_cols]+[F.stddev(c).alias(c+'_std') for c in outlier_cols]
# ).collect()
df.select(
    [(F.mean(c)+3*F.stddev(c)).alias(c+'_ceil') for c in outlier_cols]
).show()

# 根据条件给数据打标签
df.select(*bounds,
          *[F.when(~F.col(c).between(bounds[c]['min'], bounds[c]['max']), "yes").otherwise("no").alias(c+'_outlier') for c in bounds])

# cut feature into bins, bucketizer, buckets, discretization
from pyspark.ml.feature import Bucketizer
bucketizer = Bucketizer(splits=[ 0, 6, 18, 60, float('Inf') ],inputCol="ages", outputCol="buckets")
df_buck = bucketizer.setHandleInvalid("keep").transform(df)

# bins, method2
from functools import reduce
splits = [0, 5, 9, 10, 11]
splits = list(enumerate(splits))  # [(0, 0), (1, 5), (2, 9), (3, 10), (4, 11)]
bins = reduce(lambda c, i: c.when(F.col('Age') <= i[1], i[0]), splits, F.when(F.col('Age') < splits[0][0], None)).otherwise(splits[-1][0] + 1).alias('bins')
df = df.select('age', bins)

# 基于聚类的特征处理，根据数据本身的特性，对特征进行聚类
# 剔除异常值后再聚类

from pyspark.ml.evaluation import ClusteringEvaluator

# feature_result, 记录每个特征的聚类结果
feature_result = {}
# bounds_col, 记录每个特征的阈值
bounds_col = {}

for col in numeric_features:

    # 计算特征的异常阈值
    quantiles = new_df.approxQuantile(col,[0.25,0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    bounds_col[col] = [
        quantiles[0] - 1.5*IQR,
        quantiles[1] + 1.5*IQR
    ]
    print("col: {}, floor: {}, ceil: {}".format(col, bounds_col[col][0], bounds_col[col][1]))

    # 根据阈值筛选数据，剔除异常值后再聚类
    temp_data = new_df.select(col).where(F.col(col) < bounds_col[col][1])
    assembler_numeric = ft.VectorAssembler(inputCols=[col], outputCol="temp_features")
    temp_data=assembler_numeric.transform(temp_data)

    silhouette_score={}
    evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='temp_features', metricName='silhouette', distanceMeasure='squaredEuclidean')
    if temp_data.count():
        for i in range(2, 5):
            KMeans_algo=KMeans(featuresCol='temp_features', k=i)
            KMeans_fit=KMeans_algo.fit(temp_data)
            output=KMeans_fit.transform(temp_data)
            # 轮廓系数
            score=evaluator.evaluate(output)
            silhouette_score[i] = score
            print("col: {}, k: {}, Silhouette Score: {}".format(col, i, score))
            # 各簇的特征分布以及用户数
            output.groupBy("prediction").agg(F.min(col), F.max(col), F.mean(col), F.count(col)).show()
        feature_result[col] = silhouette_score
    else:
        # 如果剔除了异常值后，没有数，说明该异常值的就是特征的最小值，该特征其实是只有一个unique value
        feature_result[col] = {1:1}