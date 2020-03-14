#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

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

# filter
rawDataWithHeader = sc.textFile('/test/train.tsv')
r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
header = r_lines.first()
lines = r_lines.filter(lambda x: x != header)
lines_to_rows = lines.map(lambda p: [p[3], list(map(lambda x: float(x) if x != "?" else 0, [p[4]]))[0]])
lines_to_rows.filter(lambda r: r[0] == "business" and r[1] > 0.7).take(2)

# order ascending
lines_to_rows.takeOrdered(5, key=lambda x: x[1])
# order desc
lines_to_rows.takeOrdered(5, key=lambda x: -1 * x[1])
# order by multiple fields
lines_to_rows.takeOrdered(5, key=lambda x: (-x[1], x[0]))

# unique data
lines_to_rows.map(lambda x: x[0]).distinct().collect()
# multiple fields distinct
lines_to_rows.map(lambda x: (x[0], x[1])).distinct().take(10)

# group by
lines_to_rows.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).collect()
