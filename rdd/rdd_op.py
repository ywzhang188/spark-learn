#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

sc = spark.sparkContext

# broadcast
kvFruit = sc.parallelize([(1, "orange"), (2, "apple"), (3, "banana")])
fruitMap = kvFruit.collectAsMap()
bcFruitMap = sc.broadcast(fruitMap)
fruitIds = sc.parallelize([2, 1, 3])
fruitNames = fruitIds.map(lambda x: bcFruitMap.value[x]).collect()

# accumulator
intRDD = sc.parallelize([3, 1, 2, 5, 5])
total = sc.accumulator(0.0)
num = sc.accumulator(0)
intRDD.foreach(lambda i: [total.add(i), num.add(1)])
avg = total.value / num.value

# rdd persistence
intRddMemory = sc.parallelize([3, 2, 1, 5, 5])
intRddMemory.persist()
intRddMemory.is_cached

intRddMemory.unpersist()
intRddMemory.is_cached

# set persistence level
import pyspark

intRddMemoryAndDisk = sc.parallelize([3, 1, 2, 5, 5])
intRddMemoryAndDisk.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
intRddMemoryAndDisk.is_cached
