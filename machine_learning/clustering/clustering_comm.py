#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vectors
import numpy as np

from sklearn.datasets import load_iris

X, y = load_iris(True)
data_array = np.concatenate((X, y.reshape(-1, 1)), axis=1)
ds = spark.createDataFrame(data_array.tolist(), load_iris().feature_names.append('target'))


def transData(data):
    return data.rdd.map(lambda r: [Vectors.dense(r[:-1]), r[-1]]).toDF(['features', 'label'])


transformed = transData(ds)

# Deal With Categorical Variables
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer = VectorIndexer(inputCol="features",
                               outputCol="indexedFeatures",
                               maxCategories=4).fit(transformed)
data = featureIndexer.transform(transformed)
