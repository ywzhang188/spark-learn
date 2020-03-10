#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'
import sys
from pyspark.mllib.recommendation import MatrixFactorizationModel
import os
from pyspark.sql import SparkSession


def PrepareData(sc):
    print("read movie id and names")
    itemRDD = sc.textFile("/test/movies.dat")
    itemRDD.count()
    movieTitleMap = itemRDD.map(lambda line: line.split("::")).map(lambda x: (int(x[0]), x[1])).collectAsMap()
    return movieTitleMap


def Recommend(model):
    if sys.argv[1] == "--U":
        RecommendMovies(model, movieTitleMap, int(sys.argv[2]))
    if sys.argv[1] == "--M":
        RecommendUsers(model, movieTitleMap, int(sys.argv[2]))


def RecommendMovies(model, movieTitleMap, inputUserID):
    RecommendMovies = model.recommendProducts(inputUserID, 10)
    bcMovieTitleMap = sc.broadcast(movieTitleMap)
    movie_ids = sc.parallelize([x[1] for x in RecommendMovies])
    movie_names = movie_ids.map(lambda x: bcMovieTitleMap.value[x]).collect()
    print("for user {}, we recommend movies as follows {}".format(inputUserID, movie_names))


def RecommendUsers(model, movieTitleMap, inputMovieID):
    RecommendUsers = model.recommendUsers(inputMovieID, 10)
    print("for movie {}, we recommend users as follows{}".format(movieTitleMap[inputMovieID],
                                                                 [x[1] for x in RecommendUsers]))


def LoadModel(sc):
    try:
        model = MatrixFactorizationModel.load(sc, '/models/ALSmodel')
    except Exception:
        print("Faild to load model")
    else:
        print("model loaded")
    return model


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("please input 2 params")
        exit(-1)

    SPARK_HOME = '/root/apps/spark-2.4.4-bin-hadoop2.7'
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ['JAVA_HOME'] = '/root/apps/jdk1.8.0_221'
    os.environ["PYSPARK_PYTHON"] = "/root/python_envs/spark_p37/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/python_envs/spark_p37/bin/python3"
    local_spark_example_dir = "file://{}/".format("/root/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources")

    # Starting Point: SparkSession
    spark = SparkSession.builder.appName("Read Parquet Data").config("spark.some.config.option",
                                                                     "some-value").getOrCreate()

    sc = spark.sparkContext
    print("==========prepare data=============")
    movieTitleMap = PrepareData(sc)
    print("=======load_model========")
    model = LoadModel(sc)
    print("============start recommend=============")
    Recommend(model)
