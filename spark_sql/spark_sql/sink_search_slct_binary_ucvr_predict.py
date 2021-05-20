#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USAGE: 所有品类一起训练预测的二分类概率+UCVR（候选商品池）
AUTHOR: qinzhilong2
create TIME: 2021-05-17
"""

from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import pyspark.ml.feature as ft
import pyspark.sql.functions as func
from pyspark.sql import Window
import pyspark.ml.tuning as tune
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator,BinaryClassificationEvaluator
from pyspark.ml.classification import GBTClassifier
import sys
import os
import math
import datetime
os.environ['PYSPARK_PYTHON']="/usr/local/anaconda3/bin/python3.6"

def sqlDF2pandasDF(sqlDF):
    return sqlDF.toPandas()

def pandasDF2sqlDF(spark, pandasDF):
    return spark.createDataFrame(pandasDF)

def get_best_partition(n):
    return (n // 10000) + 1

def get_best_depth(n):
    if n >= 100:
        d = max(math.ceil(math.log(n / 100, 2)), 4)
    elif n >= 10 and n < 100:
        d = 3
    else:
        d = 2
    return d

def get_best_iter(n):
    if n >= 100000:
        d = 150
    elif n >= 10000 and n < 100000:
        d = 100
    elif n >= 1000 and n < 10000:
        d = max((n // 100) + 1, 50)
    elif n >= 100 and n < 1000:
        d = max((n // 20) + 1, 10)    
    else:
        d = 5
    return d

def main(spark):
    n = len(sys.argv) - 1
    if n < 1:
        print('\nParameters are needed!!\n')
        sys.exit()
    else:
        result_type = sys.argv[1]
        sku_type = sys.argv[2]
        end_date = sys.argv[3]
        end_date_1w = sys.argv[4]
        end_date_2w = sys.argv[5]
        input_train_data_table = sys.argv[6]
        input_predict_data_table = sys.argv[7]
        output_predict_result_table = sys.argv[8]
        predict_date = sys.argv[9]

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set spark.sql.hive.mergeFiles=true")
    spark.sql("set hive.exec.orc.split.strategy=BI")
    spark.sql("set mapred.job.priority = HIGH")
    spark.sql("set hive.default.fileformat=Orc")
    spark.sql("set hive.exec.parallel=true")
    spark.sql("set hive.auto.convert.join=true")
    spark.sql("set hive.merge.mapfiles = true")
    spark.sql("set hive.merge.mapredfiles = true")
    spark.sql("set hive.merge.size.per.task = 256000000")
    spark.sql("set hive.merge.smallfiles.avgsize=128000000")
    spark.sql("set hive.merge.orcfile.stripe.level=false")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.max.dynamic.partitions=1000000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=1000000")
    spark.sql("set hive.exec.max.created.files=1000000")
    spark.sql("set mapreduce.job.counters.limit=10000")
    spark.sql("set mapred.output.compress=true")
    spark.sql("set hive.exec.compress.output=true")
    spark.sql("set spark.shuffle.service.enabled = true")
    spark.sql("set spark.sql.broadcastTimeout = 10000")


    print('end_date = {}\n'.format(end_date))
    print('sku_type = {}\n'.format(sku_type))
    print('result_type = {}\n'.format(result_type))

    ### 构建训练和预测样本

    # 确定取数口径
    if sku_type == 'old':
        sku_type_sql = ' and otc_days >= 60'
    elif sku_type == 'new':
        sku_type_sql = ' and otc_days < 60'
    else:
        sku_type_sql = ''

    # 当周正样本
    data_now = spark.sql("""
          select 
              t1.*
          from 
              (
              select * 
              from """ + input_train_data_table + """ 
              where end_date = '""" + end_date + """' and label > 0""" + sku_type_sql + """
              )t1
          join
              (
              select 
                  item_third_cate_cd
              from 
                  app.app_vdp_ai_sink_dept3_cate3_scope_mid_da
              where 
                  dt = '""" + predict_date + """'
                  and app_id = 4
                  and scene_id = 1
                  and status = 3
              group by 
                  item_third_cate_cd
              )t2
           on t1.item_third_cate_cd = t2.item_third_cate_cd
    """)

    # 提前1周的独有正样本
    data_1w = spark.sql("""
                select 
                    a.*
                from 
                    (
                    select 
                        t1.*
                    from 
                        (
                        select * 
                        from """ + input_train_data_table + """ 
                        where end_date = '""" + end_date_1w + """' and label > 0""" + sku_type_sql + """
                        )t1
                    join
                        (
                        select 
                            item_third_cate_cd
                        from 
                            app.app_vdp_ai_sink_dept3_cate3_scope_mid_da
                        where 
                            dt = '""" + predict_date + """'
                            and app_id = 4
                            and scene_id = 1
                            and status = 3
                        group by 
                            item_third_cate_cd
                        )t2
                    on t1.item_third_cate_cd = t2.item_third_cate_cd
                    )a
                left join 
                    (
                    select 
                        item_sku_id,1 as index
                    from 
                        """ + input_train_data_table + """ 
                    where 
                        end_date = '""" + end_date + """' and label > 0""" + sku_type_sql + """
                    )b 
                on 
                    a.item_sku_id=b.item_sku_id
                where 
                    index is null or index = ''
                """)

    # 提前2周的独有正样本
    data_2w = spark.sql("""
                select 
                    a.*
                from 
                    (
                    select 
                        t1.*
                    from 
                        (
                        select * 
                        from """ + input_train_data_table + """ 
                        where end_date = '""" + end_date_2w + """' and label > 0""" + sku_type_sql + """
                        )t1
                    join
                        (
                        select 
                            item_third_cate_cd
                        from 
                            app.app_vdp_ai_sink_dept3_cate3_scope_mid_da
                        where 
                            dt = '""" + predict_date + """'
                            and app_id = 4
                            and scene_id = 1
                            and status = 3
                        group by 
                            item_third_cate_cd
                        )t2
                    on t1.item_third_cate_cd = t2.item_third_cate_cd
                    )a
                left join 
                    (
                    select 
                        item_sku_id,1 as index
                    from 
                        """ + input_train_data_table + """ 
                    where 
                        end_date = '""" + end_date + """' and label > 0""" + sku_type_sql + """
                    )b 
                on 
                    a.item_sku_id=b.item_sku_id
                where 
                    index is null or index = ''
                """)

    # 合并正样本
    data = data_now.union(data_1w).union(data_2w)
    data_filter = data.filter("otc_days >= 0").filter("sku_status_cd = 3001")
    data_filter.cache()
    data_count = data_filter.count()
    print('positive data count = {}\n'.format(data_count))

    # 补充负样本
    data_neg = spark.sql("""
          select 
              t1.*
          from 
              (
              select * 
              from """ + input_train_data_table + """ 
              where end_date = '""" + end_date + """' and label = 0""" + sku_type_sql + """
              and otc_days >= 0 and sku_status_cd = 3001
              )t1
          join
              (
              select 
                  item_third_cate_cd
              from 
                  app.app_vdp_ai_sink_dept3_cate3_scope_mid_da
              where 
                  dt = '""" + predict_date + """'
                  and app_id = 4
                  and scene_id = 1
                  and status = 3
              group by 
                  item_third_cate_cd
              )t2
           on t1.item_third_cate_cd = t2.item_third_cate_cd
              """)
    data_neg.cache()
    data_neg_count = data_neg.count()
    neg_sample_ratio = min(data_count / data_neg_count, 1.0) if data_neg_count > 0 else 0.0
    data_neg_sample = data_neg.sample(neg_sample_ratio, seed=66)

    # 合并正负样本
    if result_type == 'ucvr':
        data_union = data_filter.union(data_neg_sample).orderBy(func.rand(seed=66)).filter("item_first_cate_cd is not null")\
                          .withColumn('data_type_int', func.col('data_type').cast(IntegerType())).drop('data_type').withColumnRenamed('data_type_int','data_type')\
                          .withColumn('label_adjust',func.when(func.col('label') > 1,1).otherwise(func.col('label')))\
                          .drop('label').withColumnRenamed('label_adjust','label')
    else:
        data_union = data_filter.union(data_neg_sample).orderBy(func.rand(seed=66)).filter("item_first_cate_cd is not null")\
                          .withColumn('data_type_int', func.col('data_type').cast(IntegerType())).drop('data_type').withColumnRenamed('data_type_int','data_type')\
                          .withColumn('label_binary',func.when(func.col('label') > 0,1).otherwise(0))\
                          .drop('label').withColumnRenamed('label_binary','label')	  

    # 合并sku embedding特征
    predict_date_str = ''.join(predict_date.split('-'))
    sku_vec = spark.sql("select * from tmp.tmp_qzl_sink_search_08_sku2vec_features_{0}".format(predict_date_str))
    vec_size = len(sku_vec.columns)-1
    data_union_sku2vec = data_union.join(sku_vec,on='item_sku_id',how='left')

    ### 训练模型

    # 特征分类
    # 非特征
    features_useless  = ['item_first_cate_name', 'item_second_cate_cd', 'item_second_cate_name', 'item_third_cate_cd', 'item_third_cate_name', 
             'barndname_full', 'sku_name', 'item_sku_id', 'uv_value_label', 'first_into_otc_tm', 'end_date', 'sku_status_cd','red_price','red_price_level_rank']			
    # 类别型特征
    features_catagory = ['item_first_cate_cd'] 
    # embedding特征
    features_embedding = ['sku_vec_' + str(i) for i in range(vec_size)]
    # 数值型特征
    features_numerical = [f for f in data_union_sku2vec.columns if f not in ['label'] + features_useless + features_catagory + features_embedding]		

    # 特征缺失值统计
    feature_na = data_union_sku2vec.agg(*[(1 - (func.count(c) / func.count('*'))).alias(c) for c in data_union_sku2vec.columns])
    feature_na_DF = sqlDF2pandasDF(feature_na).T
    feature_na_DF = feature_na_DF.reset_index()
    feature_na_DF.columns = ['features','na_rate']
    for i,row in feature_na_DF.iterrows():
        print('{}: {}'.format(row['features'],row['na_rate']))

    # 处理缺失值
    fillna_value = {c:-1 for c in features_numerical}
    fillna_value.update({c:-10 for c in features_embedding})  
    data_union_sku2vec_fillna = data_union_sku2vec.fillna(fillna_value)

    # 数据预处理
    stringIndexer_cd1 = ft.StringIndexer(inputCol="item_first_cate_cd", outputCol="item_first_cate_cd_index")
    encoder_cd1 = ft.OneHotEncoder(inputCol='item_first_cate_cd_index', outputCol='item_first_cate_cd_vec') 
    featuresCreator = ft.VectorAssembler(inputCols=features_numerical + [encoder_cd1.getOutputCol()] + features_embedding, outputCol='features')
    pipeline = Pipeline(stages=[stringIndexer_cd1,encoder_cd1,featuresCreator])
    data_transformer = pipeline.fit(data_union_sku2vec_fillna)
    data_transformed = data_transformer.transform(data_union_sku2vec_fillna)
    data_transformed.cache()
    data_union_count = data_transformed.count()
    print('data_union_count = {}\n'.format(data_union_count))
    data_filter.unpersist()
    data_neg.unpersist()

    p_num = get_best_partition(data_union_count)
    data_transformed = data_transformed.repartition(p_num)

    # 开始训练
    best_depth = 12 # get_best_depth(data_union_count)
    best_iter = 150 # get_best_iter(data_union_count)
    f= '1.0' # '0.8'
    s = 1.0 # 0.8

    if result_type == 'ucvr':
        gbdt = GBTRegressor(featuresCol='features',labelCol='label',predictionCol='prediction',lossType='squared',seed=66,maxMemoryInMB=2048,cacheNodeIds=True, \
                             maxDepth=best_depth,maxIter=best_iter,featureSubsetStrategy=f,subsamplingRate=s,stepSize=0.01)
    else:
        gbdt = GBTClassifier(featuresCol='features',labelCol='label',predictionCol='prediction',lossType='logistic',seed=66,maxMemoryInMB=2048,cacheNodeIds=True,\
                             maxDepth=best_depth,maxIter=best_iter,featureSubsetStrategy=f,subsamplingRate=s,stepSize=0.01)

    gbdt_model = gbdt.fit(data_transformed)

    ### 预测候选商品的结果

    # 构建待预测样本
    if sku_type == 'old':
        sku_type_sql_2 = ' where otc_days >= 60'
    elif sku_type == 'new':
        sku_type_sql_2 = ' where otc_days < 60'
    else:
        sku_type_sql_2 = ''

    data_test = spark.sql("select * from " + input_predict_data_table + "" + sku_type_sql_2 + "")
    data_test = data_test.withColumn('data_type_int', func.col('data_type').cast(IntegerType())).drop('data_type').withColumnRenamed('data_type_int','data_type')
    data_test.cache()
    data_test_count = data_test.count()
    print('data_test_count = {}\n'.format(data_test_count))
    data_test = data_test.repartition(get_best_partition(data_test_count))

    # 处理预测样本
    data_test_sku2vec = data_test.join(sku_vec,on='item_sku_id',how='left')
    fillna_value_test = {c:-1 for c in features_numerical}
    fillna_value_test.update({c:-10 for c in features_embedding}) 
    data_test_fillna = data_test_sku2vec.fillna(fillna_value_test)
    data_transformer_test = pipeline.fit(data_test_fillna)
    data_transformed_test = data_transformer_test.transform(data_test_fillna)
    data_transformed_test.cache()
    data_test.unpersist()

    # 得到并输出候选商品池的预测结果
    gbdt_pred_test = gbdt_model.transform(data_transformed_test)
    features_result = ['item_third_cate_cd','item_sku_id', 'prediction', 'red_price','red_price_level_rank','otc_days']

    if result_type == 'binary_prob':
        gbdt_pred_test = gbdt_pred_test.select(['item_third_cate_cd','item_sku_id','probability','red_price','red_price_level_rank','otc_days'])\
                         .rdd.map(lambda row:(row['item_third_cate_cd'],row['item_sku_id'],float(row['probability'][1]),row['red_price'],row['red_price_level_rank'],row['otc_days'])).toDF(features_result)
    else:
        gbdt_pred_test = gbdt_pred_test.withColumn('prediction_adjust',func.when(func.col('prediction') > 1,1).when(func.col('prediction') < 0,0).otherwise(func.col('prediction')))\
                          .drop('prediction').withColumnRenamed('prediction_adjust','prediction')

    result = gbdt_pred_test.select(features_result).withColumn('new_old', func.when(func.col('otc_days') < 90 ,'new').otherwise('old'))
    result.createOrReplaceTempView("result_df")
    spark.sql("""
             insert overwrite table """ + output_predict_result_table + """ 
             partition(dt='""" + predict_date + """',sku_type='""" + sku_type + """',result_type='""" + result_type + """') 
             select * from result_df
    """)

    data_transformed.unpersist()
    data_transformed_test.unpersist()

if __name__ == '__main__':
    warehouseLocation = "hdfs://ns15/user/mart_vdp/app.db/"
    spark = SparkSession.builder \
            .config("spark.sql.warehouse.dir", warehouseLocation) \
            .config("spark.executor.instances", "50") \
            .config("spark.executor.memory", "48g") \
            .config("spark.executor.cores", "24") \
            .config("spark.driver.memory", "48g") \
            .config("spark.sql.shuffle.partitions", "500") \
            .enableHiveSupport() \
            .getOrCreate()
    main(spark)
    spark.stop()