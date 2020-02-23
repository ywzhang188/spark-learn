#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

# !/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.classification.classification_comm import *

from pyspark.ml.classification import RandomForestClassifier

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)
# chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# train model
model = pipeline.fit(trainingData)

# make prediction
predictions = model.transform(testData)

# evaluation
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[-2]
print(rfModel)  # summary only
