#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

import os
import sys
from urllib.parse import urlsplit
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
os.environ['SPARK_HOME'] = '/root/apps/spark-2.4.4-bin-hadoop2.7'
sys.path.append('/root/apps/spark-2.4.4-bin-hadoop2.7/python/lib')
sys.path.append('/root/apps/spark-2.4.4-bin-hadoop2.7/python')
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/thsheep/Downloads/elasticsearch-hadoop-5.3.2/dist/elasticsearch-spark-20_2.10-5.3.2.jar pyspark-shell'
if __name__ == '__main__':
    conf = SparkConf("spark://bigdata1:7077").setAppName("read_elastisearch")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    query = """
    {

    "query": {

        "multi_match" : {

            "query" : "ios",

            "fields" : ["_all"]

        }

    }

}

    """
    es_read_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "eduaio/text",
        "es.input.json": "yes",
        "es.query": query,
        "es.nodes.wan.only": "true"
    }
    es_rdd = sc.newAPIHadoopRDD(
        inputFormatClass='org.elasticsearch.hadoop.mr.EsInputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf=es_read_conf
    )
    sqlContext.createDataFrame(es_rdd).collect()