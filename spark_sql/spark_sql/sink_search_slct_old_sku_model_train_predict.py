#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USAGE: 为多端多渠道选品的搜索排序算法准备特征数据
AUTHOR: qinzhilong2
create TIME: 2021-01-15
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
from pyspark.ml.evaluation import RegressionEvaluator
import sys
import os
import math
import datetime
os.environ['PYSPARK_PYTHON']="/usr/local/anaconda3/bin/python3.6"

# 要计算的品类
def get_scope_id_batch(i, batch, scope_id_list):
    scope_id_num = len(scope_id_list)
    begin = i * batch
    end = (i + 1) * batch
    real_end = scope_id_num if end > scope_id_num else end
    scope_id_batch = scope_id_list[begin:real_end]
    return scope_id_batch

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
        i = sys.argv[1]
        batch = sys.argv[2]
        end_date = sys.argv[3]
        end_date_1w = sys.argv[4]
        end_date_2w = sys.argv[5]
        input_train_data_table = sys.argv[6]
        input_predict_data_table = sys.argv[7]
        output_cd3_score_table = sys.argv[8]
        output_train_result_table = sys.argv[9]
        output_predict_result_table = sys.argv[10]
        predict_date = sys.argv[11]

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

    # 要计算的全部品类
    cd3_list_df = spark.sql("""
                          select 
                              item_third_cate_cd
                          from 
                              """ + input_predict_data_table + """
                          group by 
                              item_third_cate_cd
                          order by 
                              split(item_third_cate_cd,'')[length(item_third_cate_cd)-1], split(item_third_cate_cd,'')[length(item_third_cate_cd)-2], item_third_cate_cd
                          """)

    # 目前还没有计算完的品类
    # cd3_list_df = spark.sql("""
                      # select
                          # item_third_cate_cd
                      # from
                         # (
                          # select 
                              # a.item_third_cate_cd,label
                          # from
                              # (
                              # select 
                                  # item_third_cate_cd
                              # from 
                                  # """ + input_predict_data_table + """
                              # group by 
                                  # item_third_cate_cd
                              # )a
                          # left JOIN
                              # (select item_third_cate_cd, 1 as label from """ + output_cd3_score_table + """ group by item_third_cate_cd)b
                          # on 
                              # a.item_third_cate_cd=b.item_third_cate_cd
                          # )t
                      # where
                          # label is null
                      # order by 
                          # split(item_third_cate_cd,'')[length(item_third_cate_cd)-1], split(item_third_cate_cd,'')[length(item_third_cate_cd)-2], item_third_cate_cd
                          # """)

    # 是否有应该出数但是没有出数的品类
    # cd3_list_df = spark.sql("""
                    # select 
                    # t1.item_third_cate_cd
                    # from 
                    # (	select 
                            # item_third_cate_cd
                        # from 
                            # app.app_vdp_ai_sink_search_old_model_cd3_score 
                        # where 
                            # sku_count > 0)t1
                    # left join 
                    # (select item_third_cate_cd,1 as index from app.app_vdp_ai_sink_search_old_model_predict_result group by item_third_cate_cd)t2
                    # on t1.item_third_cate_cd=t2.item_third_cate_cd
                    # where index is null or index=''
                    # order by t1.item_third_cate_cd
                          # """)

    cd3_list = cd3_list_df.rdd.map(lambda row: row[0]).collect()
    cd3_list_batch = get_scope_id_batch(int(i), int(batch), cd3_list)

    for cd3 in cd3_list_batch:
        print('\ncd3 = {} 开始计算\n'.format(cd3))

        try:
            ### 验证当前品类是否跑过
            if_finish = spark.sql("select * from " + output_cd3_score_table + " where item_third_cate_cd = '" + cd3 + "'")
            if if_finish.count() > 0:
                print('already finished yet')
                continue

            ### 构建训练和预测样本

            # 当周正样本
            data_now = spark.sql("""
                      select * 
                      from """ + input_train_data_table + """ 
                      where end_date = '""" + end_date + """' and label > 0 and item_third_cate_cd = '""" + cd3 + """'
            """)

            # 提前1周的独有正样本
            data_1w = spark.sql("""
                        select 
                            a.*
                        from 
                            (
                            select 
                                *
                            from 
                                """ + input_train_data_table + """ 
                            where 
                                end_date = '""" + end_date_1w + """' and label > 0 and item_third_cate_cd = '""" + cd3 + """'
                            )a
                        left join 
                            (
                            select 
                                item_sku_id,1 as index
                            from 
                                """ + input_train_data_table + """ 
                            where 
                                end_date = '""" + end_date + """' and label > 0 and item_third_cate_cd = '""" + cd3 + """'
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
                                *
                            from 
                                """ + input_train_data_table + """ 
                            where 
                                end_date = '""" + end_date_2w + """' and label > 0 and item_third_cate_cd = '""" + cd3 + """'
                            )a
                        left join 
                            (
                            select 
                                item_sku_id,1 as index
                            from 
                                """ + input_train_data_table + """ 
                            where 
                                end_date = '""" + end_date + """' and label > 0 and item_third_cate_cd = '""" + cd3 + """'
                            )b 
                        on 
                            a.item_sku_id=b.item_sku_id
                        where 
                            index is null or index = ''
                        """)

            # 合并正样本
            data = data_now.union(data_1w).union(data_2w)
            data_filter = data.filter("otc_days >= 0").filter("sku_status_cd = 3001").filter("label <= 1")
            data_filter.cache()
            data_count = data_filter.count()

            # 构建待预测样本
            data_test = spark.sql("select * from " + input_predict_data_table + " where item_third_cate_cd = '" + cd3 + "'")
            data_test.cache()
            data_test_count = data_test.count()
            data_test = data_test.repartition(get_best_partition(data_test_count))

            # 判断是否缺少训练正样本或预测样本
            if data_count == 0 or data_test_count == 0:
                print('No train data or no predict data')
                spark.sql("""
                              insert overwrite table """ + output_cd3_score_table + """ 
                              partition(dt='""" + predict_date + """',item_third_cate_cd='""" + cd3 + """') 
                              values ({0},{1},{2},{3},{4},{5})
                          """.format(0, -1, -1, -1.0, -1.0, -1.0))
                continue 

            # 补充负样本
            data_neg = spark.sql("""
                      select * 
                      from """ + input_train_data_table + """
                      where end_date = '""" + end_date_1w + """' and label = 0 and item_third_cate_cd = '""" + cd3 + """'
                      """)
            data_neg.cache()
            data_neg_count = data_neg.count()
            neg_sample_ratio = min(data_count / data_neg_count, 1.0) if data_neg_count > 0 else 0.0
            data_neg_sample = data_neg.sample(neg_sample_ratio, seed=66)

            # 合并正负样本
            data_union = data_filter.union(data_neg_sample).orderBy(func.rand(seed=66))            

            # 合并sku embedding特征
            sku_vec = spark.sql("select * from tmp.tmp_qzl_sink_search_08_sku2vec_features")
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

            # 处理缺失值
            fillna_value = {c:-1 for c in features_numerical}
            fillna_value.update({c:-10 for c in features_embedding})  
            data_union_sku2vec_fillna = data_union_sku2vec.fillna(fillna_value)

            # 数据预处理
            featuresCreator = ft.VectorAssembler(inputCols=features_numerical + features_embedding, outputCol='features')
            pipeline = Pipeline(stages=[featuresCreator])
            data_transformer = pipeline.fit(data_union_sku2vec_fillna)
            data_transformed = data_transformer.transform(data_union_sku2vec_fillna)
            data_transformed.cache()
            data_union_count = data_transformed.count()
            data_filter.unpersist()
            data_neg.unpersist()

            p_num = get_best_partition(data_union_count)
            data_transformed = data_transformed.repartition(p_num)

            # 开始训练
            best_depth = get_best_depth(data_union_count)
            best_iter = get_best_iter(data_union_count)
            gbdt = GBTRegressor(featuresCol='features',labelCol='label',predictionCol='prediction',lossType='squared',seed=66,maxMemoryInMB=2048,cacheNodeIds=True, \
                                maxDepth=best_depth,maxIter=best_iter,featureSubsetStrategy='0.8',subsamplingRate=0.8,stepSize=0.01)
            gbdt_model = gbdt.fit(data_transformed)

            # 模型评估
            evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='label', metricName='rmse')			
            gbdt_pred = gbdt_model.transform(data_transformed)
            train_rmse = evaluator.evaluate(gbdt_pred, {evaluator.metricName: 'rmse'}) # 训练集rmse
            # 训练集label与predict的相关系数
            corr_result = gbdt_pred.corr('label', 'prediction')
            train_corr = corr_result if np.isnan(corr_result) == False else 1.0
            # 训练集label与predict的top 50%重合比例
            data_pred_df = gbdt_pred.select(['item_sku_id','label','prediction']).toPandas()
            top_n = max(int(data_union_count * 0.5), 1)
            sku_label_top = data_pred_df.sort_values(by=['label'],ascending=False)['item_sku_id'].values.tolist()[:top_n]
            sku_pred_top = data_pred_df.sort_values(by=['prediction'],ascending=False)['item_sku_id'].values.tolist()[:top_n]
            top_cover_ratio = len(set(sku_label_top) & set(sku_pred_top))/top_n

            ### 预测候选商品转化率

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
            features_result = ['item_sku_id', 'prediction', 'red_price','red_price_level_rank','otc_days']
            result = gbdt_pred_test.select(features_result).withColumn('new_old', func.when(func.col('otc_days') < 90 ,'new').otherwise('old'))
            result.createOrReplaceTempView("result_df")
            spark.sql("""
                     insert overwrite table """ + output_predict_result_table + """ 
                     partition(dt='""" + predict_date + """',item_third_cate_cd='""" + cd3 + """') 
                     select * from result_df
            """)

            # 输出训练集样本的预测结果
            features_result_train = ['item_sku_id','label','prediction']
            train_result = gbdt_pred.select(features_result_train)
            train_result.createOrReplaceTempView("train_result_df")
            spark.sql("""
                     insert overwrite table """ + output_train_result_table + """ 
                     partition(dt='""" + predict_date + """',item_third_cate_cd='""" + cd3 + """') 
                     select * from train_result_df
            """)

            # 输出品类训练模型的验证结果
            spark.sql("""
                          insert overwrite table """ + output_cd3_score_table + """ 
                          partition(dt='""" + predict_date + """',item_third_cate_cd='""" + cd3 + """') 
                          values ({0},{1},{2},{3},{4},{5})
                      """.format(data_union_count, best_depth, best_iter, train_rmse, train_corr, top_cover_ratio))

            data_transformed.unpersist()
            data_transformed_test.unpersist()

        except Exception as e:
            print('Error:', e)
            continue

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