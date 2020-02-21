#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from ml.regression.regression_comm import *
from pyspark.ml.regression import RandomForestRegressor

# Define LinearRegression algorithm
rf = RandomForestRegressor() # featuresCol="indexedFeatures",numTrees=2, maxDepth=2, seed=42

