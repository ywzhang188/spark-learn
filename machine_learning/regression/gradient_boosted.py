#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from machine_learning.regression.regression_comm import *
from pyspark.ml.regression import GBTRegressor

# Define LinearRegression algorithm
rf = GBTRegressor() #numTrees=2, maxDepth=2, seed=42

