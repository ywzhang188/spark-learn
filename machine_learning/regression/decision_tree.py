#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.regression.regression_comm import *

from pyspark.ml.regression import DecisionTreeRegressor

# Train a DecisionTree model.
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")
# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, dt])

model = pipeline.fit(trainingData)

# prediction
predictions = model.transform(testData)

# evaluation
from pyspark.ml.evaluation import RegressionEvaluator
# select (prediction, true, label) and compute test error
evalutor = RegressionEvaluator(labelCol="label", predictionCol="prediction",
                               metricName="rmse")
rmse = evalutor.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
# R2
y_true = predictions.select("label").toPandas()
y_pred = predictions.select("prediction").toPandas()

from sklearn.metrics import r2_score
r2_score = r2_score(y_true, y_pred)
print('r2_score: {0}'.format(r2_score))

# check the importance of the features
model_dt = model.stages[-1]
model_dt.featureImportances


