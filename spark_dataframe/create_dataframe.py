#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

# By using parallelize( ) function
ds = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'),
                                     (4, 5, 6, 'd e f'),
                                     (7, 8, 9, 'g h i')]).toDF(['col1', 'col2', 'col3', 'col4'])

# By using createDataFrame( ) function
ds = spark.createDataFrame([(1, 2, 3, 'a b c'),
                            (4, 5, 6, 'd e f'),
                            (7, 8, 9, 'g h i')], ['col1', 'col2', 'col3', 'col4'])
ds.show()

# By using read and load functions
# Read dataset from .csv file
ds = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';',
                     inferSchema="true", header="true")
# ds = spark.read.csv(local_spark_example_dir + "people.csv", sep=';', header=True)
ds.show(5)
ds.printSchema()

# Read dataset from DataBase
'''Reading tables from Database needs the proper drive for the corresponding Database. 
For example, the above demo needs org.postgresql.Driver and you need to download it 
and put it in jars folder of your spark installation path. I download postgresql-42.1.1.jar from the official website
and put it in jars folder.'''
# User information
user = 'your_username'
pw = 'your_password'

# Database information
table_name = 'table_name'
url = 'jdbc:postgresql://##.###.###.##:5432/dataset?user=' + user + '&password=' + pw
properties = {'driver': 'org.postgresql.Driver', 'password': pw, 'user': user}

df = spark.read.jdbc(url=url, table=table_name, properties=properties)

df.show(5)
df.printSchema()

# read dataset from HDFS
spark.read.load('hdfs:///carbon_data/sample_data.parquet').printSchema()
# spark.read.load('/carbon_data/sample_data.parquet').printSchema()

# create dataframe from RDD
rawDataWithHeader = sc.textFile('/test/train.tsv')
r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
header = r_lines.first()
lines = r_lines.filter(lambda x: x != header)
from pyspark.sql import Row

lines_to_rows = lines.map(lambda p: Row(alchemy_category=p[3],
                                        alchemy_category_score=list(map(lambda x: float(x) if x != "?" else 0, [p[4]]))[
                                            0]))
ds = spark.createDataFrame(lines_to_rows)
ds.take(2)
