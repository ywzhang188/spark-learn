#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import IDF, Tokenizer, CountVectorizer

sentenceData = spark.createDataFrame([
    (0, "Python python Spark Spark"),
    (1, "Python SQL")],
    ["document", "sentence"])

sentenceData.show(truncate=True)

# Countervectorizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
vectorizer = CountVectorizer(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
pipeline = Pipeline(stages=[tokenizer, vectorizer, idf])
model = pipeline.fit(sentenceData)

import numpy as np

rawFeatures = model.transform(sentenceData).select('rawFeatures')
rawFeatures.show(truncate=False)
total_counts = rawFeatures.rdd.map(lambda row: row['rawFeatures'].toArray()).reduce(
    lambda x, y: [x[i] + y[i] for i in range(len(y))])

vectorizerModel = model.stages[1]
vocabList = vectorizerModel.vocabulary
d = {'vocabList': vocabList, 'counts': total_counts}
spark.createDataFrame(np.array(list(d.values())).T.tolist(), list(d.keys())).show()

from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.types import ArrayType, StringType

indices_udf = udf(lambda vector: vector.indices.tolist(), ArrayType(IntegerType()))
values_udf = udf(lambda vector: vector.toArray().tolist(), ArrayType(DoubleType()))


def termsIdx2Term(vocabulary):
    def termsIdx2Term(termIndices):
        return [vocabulary[int(index)] for index in termIndices]

    return udf(termsIdx2Term, ArrayType(StringType()))


rawFeatures.withColumn('indices', indices_udf(F.col('rawFeatures'))) \
    .withColumn('values', values_udf(F.col('rawFeatures'))) \
    .withColumn("Terms", termsIdx2Term(vocabList)("indices")).show()

# HashingTF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
vectorizer  = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=5)
idf = IDF(inputCol="rawFeatures", outputCol="features")
pipeline = Pipeline(stages=[tokenizer, vectorizer, idf])
model = pipeline.fit(sentenceData)
model.transform(sentenceData).show(truncate=False)

# word embedding models in pyspark
from pyspark.ml.feature import Word2Vec
from pyspark.ml import Pipeline

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="words", outputCol="feature")
pipeline = Pipeline(stages=[tokenizer, word2Vec])
model = pipeline.fit(sentenceData)
result = model.transform(sentenceData)
result.show(truncate=False)
w2v = model.stages[1]
w2v.getVectors().show(truncate=False)
from pyspark.sql.functions import format_number as fmt
w2v.findSynonyms("could", 2).select("word", fmt("similarity", 5).alias("similarity")).show()

# FeatureHasher
from pyspark.ml.feature import FeatureHasher
dataset = spark.createDataFrame([
    (2.2, True, "1", "foo"),
    (3.3, False, "2", "bar"),
    (4.4, False, "3", "baz"),
    (5.5, False, "4", "foo")
], ["real", "bool", "stringNum", "string"])
hasher = FeatureHasher(inputCols=["real", "bool", "stringNum", "string"],
                       outputCol="features")
featurized = hasher.transform(dataset)
featurized.show(truncate=False)

# RFormula
from pyspark.ml.feature import RFormula
dataset = spark.createDataFrame(
    [(7, "US", 18, 1.0),
     (8, "CA", 12, 0.0),
     (9, "CA", 15, 0.0)],
    ["id", "country", "hour", "clicked"])

formula = RFormula(
    formula="clicked ~ country + hour",
    featuresCol="features",
    labelCol="label")

output = formula.fit(dataset).transform(dataset)
output.select("features", "label").show()