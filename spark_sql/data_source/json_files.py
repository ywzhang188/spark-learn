#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

sc = spark.sparkContext
peopleDF = spark.read.json(local_spark_example_dir+"examples/src/main/resources/people.json")

# the inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()

# creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

# sql statements can be run by using the sql methods provided by spark
teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenagerNamesDF.show()

# alternatively, a DataFrame can be created for a JSON dataset represented by an RDD storing one JSON object per string
jsonStrings = ['{"name":"Yin", "address":{"city":"Columnbus", "state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
otherPeople.show()

