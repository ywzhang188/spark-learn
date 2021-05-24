#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

sentenceDataFrame = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,neat")
], ["id", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
# alternatively, pattern="\\w+", gaps(False)
countTokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words") \
    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)

regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words") \
    .withColumn("tokens", countTokens(col("words"))).show(truncate=False)

# StopWordsRemover

from pyspark.ml.feature import StopWordsRemover

sentenceData = spark.createDataFrame([
    (0, ["I", "saw", "the", "red", "balloon"]),
    (1, ["Mary", "had", "a", "little", "lamb"])
], ["id", "raw"])

remover = StopWordsRemover(inputCol="raw", outputCol="removeded")
remover.transform(sentenceData).show(truncate=False)

# NGram
from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, Tokenizer
from pyspark.ml.feature import NGram

sentenceData = spark.createDataFrame([
    (0.0, "I love Spark"),
    (0.0, "I love python"),
    (1.0, "I think ML is awesome")],
    ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
ngram = NGram(n=2, inputCol="words", outputCol="ngrams")
idf = IDF(inputCol="rawFeatures", outputCol="features")
pipeline = Pipeline(stages=[tokenizer, ngram])
model = pipeline.fit(sentenceData)
model.transform(sentenceData).show(truncate=False)

# Binarizer
from pyspark.ml.feature import Binarizer

continuousDataFrame = spark.createDataFrame([
    (0, 0.1),
    (1, 0.8),
    (2, 0.2),
    (3, 0.5)
], ["id", "feature"])
binarizer = Binarizer(threshold=0.5, inputCol="feature", outputCol="binarized_feature")
binarizerDataFrame = binarizer.transform(continuousDataFrame)
print("Binarizer output with Threhold = %f" % binarizer.getThreshold())
binarizerDataFrame.show()

# Bucketizer
from pyspark.ml.feature import Bucketizer

data = [(0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.0)]
df = spark.createDataFrame(data, ["id", "age"])

splits = [-float("inf"), 3, 10, float("inf")]
result_bucketizer = Bucketizer(splits=splits, inputCol="age", outputCol="result").transform(df)
result_bucketizer.show()

# QuantileDiscretizer
from pyspark.ml.feature import QuantileDiscretizer

data = [(0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.0)]
df = spark.createDataFrame(data, ["id", "age"])
print(df.show())

qds = QuantileDiscretizer(numBuckets=5, inputCol="age", outputCol="buckets",
                          relativeError=0.01, handleInvalid="error")
bucketizer = qds.fit(df)
bucketizer.transform(df).show()
bucketizer.setHandleInvalid("skip").transform(df).show()

discretizers = [ft.QuantileDiscretizer(inputCol=c, outputCol="{}_buckets".format(c))
                for c in numeric_features]
# discretizers = ft.QuantileDiscretizer(numBuckets=3,inputCols=numeric_features, outputCols=["{}_buckets".format(c) for c in numeric_features])


# StringIndexer
from pyspark.ml.feature import StringIndexer

df = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexer = indexer.fit(df).transform(df)
indexer.show()

# labelConveter
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer

df = spark.createDataFrame(
    [(0, "Yes"), (1, "Yes"), (2, "Yes"), (3, "No"), (4, "No"), (5, "No")],
    ["id", "label"])

indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
converter = IndexToString(inputCol="labelIndex", outputCol="originalLabel")
pipeline = Pipeline(stages=[indexer, converter])
model = pipeline.fit(df)
result = model.transform(df)
result.show()

# VectorIndexer
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import RFormula

df = spark.createDataFrame([
    (0, 2.2, True, "1", "foo", 'CA'),
    (1, 3.3, False, "2", "bar", 'US'),
    (0, 4.4, False, "3", "baz", 'CHN'),
    (1, 5.5, False, "4", "foo", 'AUS')
], ['label', "real", "bool", "stringNum", "string", "country"])

formula = RFormula(formula="label ~ real + bool + stringNum + string + country",
                   featuresCol="features",
                   labelCol="label")
featureIndexer = VectorIndexer(inputCol="features", \
                               outputCol="indexedFeatures", \
                               maxCategories=2)
pipeline = Pipeline(stages=[formula, featureIndexer])
model = pipeline.fit(df)
result = model.transform(df)
result.show()

# VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

dataset = spark.createDataFrame(
    [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
    ["id", "hour", "mobile", "userFeatures", "clicked"])

assembler = VectorAssembler(
    inputCols=["hour", "mobile", "userFeatures"],
    outputCol="features")

output = assembler.transform(dataset)
print("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
output.select("features", "clicked").show(truncate=False)

# OneHotEncoder
from pyspark.ml.feature import OneHotEncoder, StringIndexer

df = spark.createDataFrame([
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
], ["id", "category"])
stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = stringIndexer.fit(df)
indexed = model.transform(df)
# default setting: dropLast=True
encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec", dropLast=False)
encoded = encoder.transform(indexed)
encoded.show()

categoricalCols = ['category']
indexers = [StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)) for c in categoricalCols]
# default setting: dropLast=True
encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(),
                          outputCol="{0}_encoded".format(indexer.getOutputCol()), dropLast=False)
            for indexer in indexers]
assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                            , outputCol="features")
pipeline = Pipeline(stages=indexers + encoders + [assembler])

model = pipeline.fit(df)
data = model.transform(df)


# application: get dummy variable
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


# supervised senario
df = spark.createDataFrame([
    (0, "a", 8, 1),
    (1, "b", 10, 0),
    (2, "c", 23, 1),
    (3, "a", 11, 1),
    (4, "a", 9, 0),
    (5, "c", 15, 1)
], ["id", "category", "scores", "label"])
df.show()

indexCol = 'id'
catCols = ['category']
contCols = ['scores']
labelCol = 'label'
data = get_dummy(df, indexCol, catCols, contCols, labelCol, dropLast=False)
data.show(5)

# scaler
from pyspark.ml.feature import Normalizer, StandardScaler, MinMaxScaler, MaxAbsScaler

scaler_type = 'Normal'
if scaler_type == "Normal":
    scaler = Normalizer(inputCol="features", outputCol="scaledFeatures", p=1.0)
elif scaler_type == "Standard":
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
elif scaler_type == "MinMaxScaler":
    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
elif scaler_type == 'MaxAbsScaler':
    scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
], ["id", "features"])
df.show()
pipeline = Pipeline(stages=[scaler])
model = pipeline.fit(df)
data = model.transform(df)
data.show()

# Normalizer
from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors

dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
], ["id", "features"])
# Normalize each Vector using $L^1$ norm.
normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
l1NormData = normalizer.transform(dataFrame)
print("Normalized using L^1 norm")
l1NormData.show()
# Normalize each Vector using $L^1\infty$ norm
lInfNormData = normalizer.transform(dataFrame, {normalizer.p: float("inf")})
print("Normalized using L^inf norm")
lInfNormData.show()

# StandardScaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
scaleredData = scaler.fit(dataFrame).transform(dataFrame)
scaleredData.show(truncate=False)

# MinMaxScaler
scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
scaledData = scaler.fit(dataFrame).transform(dataFrame)
scaledData.show(truncate=False)

# MaxAbsScaler
scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")
scaledData = scaler.fit(dataFrame).transform(dataFrame)
scaledData.show(truncate=False)

# PCA
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors

data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
        (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
        (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
df = spark.createDataFrame(data, ["features"])
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)
result = model.transform(df).select("pcaFeatures")
result.show(truncate=False)

# DCT
from pyspark.ml.feature import DCT
from pyspark.ml.linalg import Vectors
df = spark.createDataFrame([
    (Vectors.dense([0.0, 1.0, -2.0, 3.0]),),
    (Vectors.dense([-1.0, 2.0, 4.0, -7.0]),),
    (Vectors.dense([14.0, -2.0, -5.0, 1.0]),)], ["features"])
dct = DCT(inverse=False, inputCol="features", outputCol="featuresDCT")
dctDf = dct.transform(df)
dctDf.select("featuresDCT").show(truncate=False)
