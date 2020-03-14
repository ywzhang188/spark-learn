#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

df = spark.createDataFrame([
    (0, "Yes"),
    (1, "Yes"),
    (2, "Yes"),
    (3, "Yes"),
    (4, "No"),
    (5, "No")
], ["id", "label"])
df.show()

# calculate undersampling ratio
import math


def round_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier


df = df.dropna()
# unique values
df.select('label').distinct().show()
label_Y = df.filter(df['label'] == 'Yes')
label_N = df.filter(df['label'] == 'No')
sampleRatio = round_up(label_N.count() / df.count(), 2)

# undersampling
label_Y_sample = label_Y.sample(False, sampleRatio)
data = label_N.unionAll(label_Y_sample)
data.show()
