#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.clustering.clustering_comm import *

from pyspark.ml.clustering import KMeans

# Elbow method to determine the optimal number of clusters for k-means clustering
import numpy as np

cost = np.zeros(20)
for k in range(2, 20):
    kmeans = KMeans() \
        .setK(k) \
        .setSeed(1) \
        .setFeaturesCol("indexedFeatures") \
        .setPredictionCol("cluster")

    model = kmeans.fit(data)
    cost[k] = model.computeCost(data)  # requires Spark 2.0 or later

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import seaborn as sbs
from matplotlib.ticker import MaxNLocator

fig, ax = plt.subplots(1, 1, figsize=(8, 6))
ax.plot(range(2, 20), cost[2:20])
ax.set_xlabel('k')
ax.set_ylabel('cost')
ax.xaxis.set_major_locator(MaxNLocator(integer=True))
plt.savefig('./kmeans.png')


# Silhouette analysis

def optimal_k(df_in, k_min, k_max, num_runs):
    '''
    Determine optimal number of clusters by using Silhoutte Score Analysis.
    :param df_in: the input dataframe
    :param index_col: the name of the index column
    :param k_min: the train dataset
    :param k_min: the minmum number of the clusters
    :param k_max: the maxmum number of the clusters
    :param num_runs: the number of runs for each fixed clusters

    :return k: optimal number of the clusters
    :return silh_lst: Silhouette score
    :return r_table: the running results table
    '''
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    import time
    import pandas as pd

    start = time.time()
    silh_lst = []
    k_lst = np.arange(k_min, k_max + 1)

    r_table = pd.DataFrame(index=range(data.count()))
    centers = pd.DataFrame()

    for k in k_lst:
        silh_val = []
        for run in np.arange(1, num_runs + 1):
            # Trains a k-means model.
            kmeans = KMeans() \
                .setK(k) \
                .setSeed(int(np.random.randint(100, size=1)))
            model = kmeans.fit(df_in)

            # Make predictions
            predictions = model.transform(df_in)
            r_table['cluster_{k}_{run}'.format(k=k, run=run)] = predictions.select('prediction').toPandas()

            # Evaluate clustering by computing Silhouette score
            evaluator = ClusteringEvaluator()
            silhouette = evaluator.evaluate(predictions)
            silh_val.append(silhouette)

        silh_array = np.asanyarray(silh_val)
        silh_lst.append(silh_array.mean())

    elapsed = time.time() - start

    silhouette = pd.DataFrame(list(zip(k_lst, silh_lst)), columns=['k', 'silhouette'])

    print('+------------------------------------------------------------+')
    print("|         The finding optimal k phase took %8.0f s.       |" % (elapsed))
    print('+------------------------------------------------------------+')

    return k_lst[np.argmax(silh_lst, axis=0)], silhouette, r_table


k, silh_lst, r_table = optimal_k(transformed, 2, 20, 2)
spark.createDataFrame(silh_lst).show()

# pipeline
from pyspark.ml.clustering import KMeans, KMeansModel

kmeans = KMeans() \
    .setK(3) \
    .setFeaturesCol("indexedFeatures") \
    .setPredictionCol("cluster")

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[featureIndexer, kmeans])

model = pipeline.fit(transformed)

cluster = model.transform(transformed)

# train and evaluate kmeans model
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as F

def train_cluster(df, k):
    evaluator = ClusteringEvaluator(predictionCol='cluster', featuresCol='final_features_scaled', \
                                    metricName='silhouette', distanceMeasure='squaredEuclidean')
    kmeans = KMeans() \
        .setK(k) \
        .setFeaturesCol("final_features_scaled") \
        .setPredictionCol("cluster")

    kmeans_model = kmeans.fit(df)

    output = kmeans_model.transform(df)

    score=evaluator.evaluate(output)
    print("k: {}, silhouette score: {}".format(k, score))
    expr_mean = [F.avg(col).alias(col+'_mean') for col in final_features]

    #     @pandas_udf(FloatType(), functionType=PandasUDFType.GROUPED_AGG)
    #     def _func_median(v):
    #         return v.median()
    #     expr_median = [_func_median(output[col]).alias(col+'_median') for col in numeric_features]
    #     df_median = output.groupBy('cluster').agg(*expr_median).toPandas()
    df_mean = output.groupBy('cluster').agg(F.count(F.lit(1)).alias("audience_num"), *expr_mean).toPandas()
    #     result = pd.merge(df_mean, df_median, on='cluster')
    return output, df_mean
