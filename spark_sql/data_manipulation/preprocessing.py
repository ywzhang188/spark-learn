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