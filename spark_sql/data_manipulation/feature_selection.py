#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *
df = spark.createDataFrame([
    (0, "a", 8, 1),
    (1, "b", 10, 0),
    (2, "c", 23, 1),
    (3, "a", 11, 1),
    (4, "a", 9, 0),
    (5, "c", 15, 1)
], ["id", "category", "scores", "label"])
df.show()

# convert the data to dense vector
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
# method 1 (good for small feature):
def transData(row):
    return Row(label=row["Sales"],
               features=Vectors.dense([row["TV"],
                                       row["Radio"],
                                       row["Newspaper"]]))

# Method 2 (good for large features):
def transData2(data):
    return data.rdd.map(lambda r: [Vectors.dense(r[:-1]), r[-1]]).toDF(['features', 'label'])

