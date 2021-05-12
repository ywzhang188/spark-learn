#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='zhangyuwei37'

import pyspark.ml.feature as ft
from pyspark.ml import Pipeline

# 特征预处理：对类别变量onehot,对数值变量scaling, 最后整合特征，输出pca降维结果
# onehot
indexers = [ft.StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
            for c in nomial_features]
encoders = [ft.OneHotEncoder(inputCol=indexer.getOutputCol(),
                          outputCol="{0}_encoded".format(indexer.getOutputCol()))
            for indexer in indexers]
assembler_onehot = ft.VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders], outputCol="onehot_features")

#scaler
assembler_numeric = ft.VectorAssembler(inputCols=numeric_features, outputCol="numeric_features")
std_scaler = ft.StandardScaler(inputCol="numeric_features", outputCol="numeric_features_scaled")

assembler_final = ft.VectorAssembler(inputCols=['onehot_features', 'numeric_features_scaled'], outputCol="final_features")

pca_model = ft.PCA(k=6, inputCol = "final_features", outputCol = "pca_features")

pipeline = Pipeline(stages=indexers + encoders + [assembler_onehot, assembler_numeric, std_scaler, assembler_final, pca_model])
preprocess_model = pipeline.fit(df)
scaledData = preprocess_model.transform(df)

# 保存和加载模型，save model load model
from pyspark.ml import PipelineModel
outpath = "/dbfs/classification_models/model-maxDepth{}-maxBins{}".format(MAXDEPTH, MAXBINS)

pipelineModel.write().overwrite().save(outpath)
model_in = PipelineModel.load(outpath)