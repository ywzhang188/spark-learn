#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from ml.regression.regression_comm import *

# defime linearRegression class
from pyspark.ml.regression import LinearRegression

lr = LinearRegression()

# pipeline architecture
# chain indexer and tree in a pipeline
pipeline = Pipeline(stages=[featureIndexer, lr])
model = pipeline.fit(trainingData)


modelsummary(model.stages[-1])

# make prediction
model_r = model.stages[-1]
predictions = model_r.transform(testData)
predictions.show(5)

# evaluation
from pyspark.ml.evaluation import RegressionEvaluator

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(labelCol="label",
                                predictionCol="prediction",
                                metricName="rmse")

rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# check R2
y_true = predictions.select("label").toPandas()
y_pred = predictions.select("prediction").toPandas()

from sklearn.metrics import r2_score
r2_score = r2_score(y_true, y_pred)
print('r2_score: {0}'.format(r2_score))
