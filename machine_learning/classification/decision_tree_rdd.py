#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

rawDataWithHeader = sc.textFile('/test/train.tsv')

# data processing
r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
header = r_lines.first()
lines = r_lines.filter(lambda x: x != header)

# extract feature
import numpy as np


def extract_features(fields, categoriesMap, featureEnd):
    categoryIdx = categoriesMap[fields[3]]
    categoryFeatures = np.zeros(len(categoriesMap))
    categoryFeatures[categoryIdx] = 1
    # 提取数值字段
    numericalFeatures = [convert_float(field) for field in fields[4:featureEnd]]
    return np.concatenate((categoryFeatures, numericalFeatures))


def convert_float(x):
    return 0 if x == "?" else float(x)


categoriesMap = lines.map(lambda fields: fields[3]).distinct().zipWithIndex().collectAsMap()


def extract_label(fields):
    label = fields[-1]
    return label


from pyspark.mllib.regression import LabeledPoint

labelPointRDD = lines.map(lambda r: LabeledPoint(extract_label(r), extract_features(r, categoriesMap, -1)))

trainData, validationData, testData = labelPointRDD.randomSplit([8, 1, 1])

# temporary save data into memory to speed up the later process
trainData.persist()
validationData.persist()
testData.persist()

# train model
from pyspark.mllib.tree import DecisionTree

model = DecisionTree.trainClassifier(trainData, numClasses=2, categoricalFeaturesInfo={}, impurity="entropy",
                                     maxDepth=5, maxBins=5)


# evaluate model
def evaluate_model(model, validationData):
    from pyspark.mllib.evaluation import BinaryClassificationMetrics
    score = model.predict(validationData.map(lambda p: p.features))
    scoreAndLabels = score.zip(validationData.map(lambda p: p.label))
    scoreAndLabels.take(5)
    metrics = BinaryClassificationMetrics(scoreAndLabels)
    print("auc: ", metrics.areaUnderROC)
    return metrics.areaUnderROC


AUC = evaluate_model(model, validationData)


# predict data
def PredictData(sc, model, categoriesMap):
    rawDataWithHeader = sc.textFile('/test/test.tsv')

    # data processing
    r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
    header = r_lines.first()
    lines = r_lines.filter(lambda x: x != header)
    dataRDD = lines.map(lambda r: (r[0], extract_features(r, categoriesMap, len(r))))
    DescDict = {
        0: "ephemeral",
        1: "evergreen"
    }
    for data in dataRDD.take(10):
        predictResult = model.predict(data[1])
        print(data[0], DescDict[predictResult])


# improve model
# define train evaluate model func
from time import time


def train_evaluate(trainData, validationData, impurityParm, maxDepthParm, maxBinsParm):
    startTime = time()
    model = DecisionTree.trainClassifier(trainData, numClasses=2, categoricalFeaturesInfo={}, impurity=impurityParm,
                                         maxDepth=maxDepthParm, maxBins=maxBinsParm)
    AUC = evaluate_model(model, validationData)
    duration = time() - startTime
    print("impurity: {}, maxDepth: {}, maxBins: {}, time_consumption: {}, AUC: {}".format(impurityParm, maxDepthParm,
                                                                                          maxBinsParm, duration, AUC))
    return (AUC, duration, impurityParm, maxDepthParm, maxBinsParm, model)


impurity_list = ["gini", "entropy"]
max_depth_list = [15, 10, 8]
max_bins_list = [15, 10, 8]

metrics = [train_evaluate(trainData, validationData, impurity, maxDepth, maxBins) for impurity in impurity_list for
           maxDepth in max_depth_list for maxBins in max_bins_list]
bestParameters = sorted(metrics, key=lambda k: k[0], reverse=True)[0]

import pandas as pd

metrics_df = pd.DataFrame(metrics, columns=['AUC', 'duration', 'impurityParm', 'maxDepthParm', 'maxBinsParm', 'model'])

# visualization
import matplotlib.pyplot as plt


def evaluate_plot(df, evalparm, barData, lineData, yMin, yMax):
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_ylim([yMin, yMax])
    ax1.set_ylabel(barData, fontsize=12, color=color)
    ax1.bar(df[evalparm].values, df[barData].values, color=color)
    ax1.set_xticklabels(df[evalparm].values, rotation=30)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()
    color = 'tab:blue'
    ax2.set_ylabel(lineData, fontsize=12, color=color)
    ax2.plot(df[lineData].values, linestyle='-', marker='o', linewidth=2.0, color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    plt.savefig('./evaluate.png')


metrics_df['impurity'] = metrics_df.impurityParm + metrics_df.index.astype(str)
evaluate_plot(metrics_df, "impurity", "AUC", "duration", 0.5, 0.7)

# test model on test data
AUC = evaluate_model(model, testData)
