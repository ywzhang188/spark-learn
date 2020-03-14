#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# interoperating with RDDs
sc = spark.sparkContext
# load a text file and convert each line to a Row
lines = sc.textFile("file://{}".format(os.path.join(SPARK_HOME, "examples/src/main/resources/people.txt")))
parts = lines.map(lambda l: l.split(","))
# each line is converted to a tuple
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
# Infer the schema, and register the DataFrame as a table
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")
# The results of SQL queries are Dataframe objects.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
# field alias
teenagers = spark.sql("SELECT age*2 double_age  FROM people")

# group by
spark.sql("SELECT name, count(*) counts FROM people GROUP BY name").show()
spark.sql("SELECT name, age, count(*) counts FROM people GROUP BY name, age").show()

# filter
teenagers = spark.sql("SELECT age*2 double_age  FROM people WHERE age >= 13")

teenagers.show()
teenagers.toPandas()
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
# teenNames is a list
for name in teenNames:
    print(name)

# programmatically specifying the schema
from pyspark.sql.types import *

sc = spark.sparkContext
# Load a text file and convert each line to a Row,从原始RDD创建元组
lines = sc.textFile("file://{}".format(os.path.join(SPARK_HOME, "examples/src/main/resources/people.txt")))
parts = lines.map(lambda l: l.split(","))
# converted to a row
people = parts.map(lambda p: (p[0], p[1].strip()))
# the schema is encoded in a string创建一个由StructType表示的模式来匹配元组或列表的结构
schemaString = "name age"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
# apply
schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.createOrReplaceTempView("people")
results = spark.sql("SELECT name FROM people")
results.show()

# table join
spark.sql(
    """SELECT U.*, z.city, z.state 
    FROM user_table u 
    LEFT JOIN zipcode_table z 
    ON u.zipcpde=z.zipcode 
    WHERE z.state='NY'
    """).show(10)
