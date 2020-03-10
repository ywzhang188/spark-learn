#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'


from spark_sql.getting_started.spark_session import *

sc = spark.sparkContext

textFile = sc.textFile("/test/word_count.txt")
stringRDD = textFile.flatMap(lambda line: line.split(" "))
countsRDD = stringRDD.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
countsRDD.collect()
# countsRDD.foreach(print)

# save result
countsRDD.saveAsTextFile("/test/wc_output")
# countsRDD.saveAsTextFile("file:///root/PycharmProjects/spark-learn/wc_output")
