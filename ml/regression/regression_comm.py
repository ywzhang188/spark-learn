#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

ds = spark.createDataFrame([(1, 2, 3, 10),
                            (0, 5, 6, 11),
                            (1, 8, 9, 8),
                            (2, 3, 7, 12),
                            (1, 4, 9, 6)], ['x1', 'x2', 'x3', 'y'])


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

# split data into training and test sets
(trainingData, testData) = transformed.randomSplit([0.7, 0.3])

# summary of the model
def modelsummary(model):
    import numpy as np
    print("Note: the last rows are the information for Intercept")
    print("##", "-------------------------------------------------")
    print("##", "  Estimate   |   Std.Error | t Values  |  P-value")
    coef = np.append(list(model.coefficients), model.intercept)
    Summary = model.summary

    for i in range(len(Summary.pValues)):
        print("##", '{:10.6f}'.format(coef[i]), \
              '{:10.6f}'.format(Summary.coefficientStandardErrors[i]), \
              '{:8.3f}'.format(Summary.tValues[i]), \
              '{:10.6f}'.format(Summary.pValues[i]))

    print("##", '---')
    print("##", "Mean squared error: % .6f" \
          % Summary.meanSquaredError, ", RMSE: % .6f" \
          % Summary.rootMeanSquaredError)
    print("##", "Multiple R-squared: %f" % Summary.r2, ", \
            Total iterations: %i" % Summary.totalIterations)
