#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.regression.regression_comm import *
from pyspark.ml.regression import RandomForestRegressor

# Define LinearRegression algorithm
rf = RandomForestRegressor()  # featuresCol="indexedFeatures",numTrees=2, maxDepth=2, seed=42

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, rf])
model = pipeline.fit(trainingData)

# make prediction
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("features", "label", "prediction").show(5)

# evaluation
# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# R2
y_true = predictions.select("label").toPandas()
y_pred = predictions.select("prediction").toPandas()
from sklearn.metrics import r2_score

r2_score = r2_score(y_true, y_pred)
print('r2_score: {0}'.format(r2_score))

# feature importances
model_rf = model.stages[-1]
model_rf.featureImportances
model_rf.trees
