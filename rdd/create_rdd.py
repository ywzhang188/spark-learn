#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

# By using parallelize( ) function
df = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'),
                                     (4, 5, 6, 'd e f'),
                                     (7, 8, 9, 'g h i')]).toDF(['col1', 'col2', 'col3', 'col4'])

# By using createDataFrame( ) function
df = spark.createDataFrame([(1, 2, 3, 'a b c'),
                            (4, 5, 6, 'd e f'),
                            (7, 8, 9, 'g h i')], ['col1', 'col2', 'col3', 'col4'])
df.show()

# By using read and load functions
# Read dataset from .csv file
df = spark.read.load(local_spark_example_dir + "people.csv", format='csv', sep=';',
                     inferSchema="true", header="true")
# ds = spark.read.csv(local_spark_example_dir + "people.csv", sep=';', header=True)
df.show(5)
df.printSchema()

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
