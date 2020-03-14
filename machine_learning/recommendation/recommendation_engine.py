#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'


from getting_started.spark_session import *

sc = spark.sparkContext

rawUserData = sc.textFile('/test/ratings.dat')
rawUserData.first()

rawRatings = rawUserData.map(lambda line: line.split("::")[:3])
rawRatings.take(5)
ratingsRDD = rawRatings.map(lambda x: (x[0], x[1], x[2]))
ratingsRDD.take(5)
numRatings = ratingsRDD.count()
numUsers = ratingsRDD.map(lambda x: x[0]).distinct().count()
numMovies = ratingsRDD.map(lambda x: x[1]).distinct().count()

# MatrixFactorizationModel
from pyspark.mllib.recommendation import ALS

# Explicit Rating
model = ALS.train(ratingsRDD, rank=10, iterations=10, lambda_=0.01)
# 给用户推荐可能感兴趣产品
model.recommendProducts(user=100, num=5)
# 查看针对用户推荐产品的评分
model.predict(user=100, product=2503)
# recommend products to users
model.recommendUsers(product=200, num=10)

# show suggested movie name
itemRDD = sc.textFile("/test/movies.dat")
itemRDD.count()
movieTitleMap = itemRDD.map(lambda line: line.split("::")).map(lambda x: (int(x[0]), x[1])).collectAsMap()
len(movieTitleMap)
list(movieTitleMap.items())[:5]

bcMovieTitleMap = sc.broadcast(movieTitleMap)
recommendP = model.recommendProducts(user=100, num=5)
movie_ids = sc.parallelize([x[1] for x in recommendP])
movie_names = movie_ids.map(lambda x: bcMovieTitleMap.value[x]).collect()


# save model
def SaveModel(sc, model):
    try:
        model.save(sc, '/models/ALSmodel')
    except Exception:
        print("Model already exists")


SaveModel(sc, model)
