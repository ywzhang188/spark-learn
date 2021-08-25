#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='zhangyuwei37'

# 将表写进数据库
df.createOrReplaceTempView("df")
df = spark.sql("""
            insert overwrite table temp.temp_test_table
            select *
            from df
        """)

# 创建表-存表
temp_view = "quanren_top_user"
tableName = "app_{}".format(temp_view)
result.registerTempTable(temp_view)
spark.sql("""
            CREATE TABLE IF NOT EXISTS app.""" + tableName + """
            ( 
                 user_log_acct string comment '用户pin',
                 user_level string comment '用户等级',
                 user_type string comment '用户类型'
             )
            COMMENT 'C2M人群圈选PIN包用户'
            PARTITIONED BY(dt string)
            STORED AS ORC
            LOCATION '/user/mart_vdp/app/vdp_user/""" + tableName + """'
         """)
spark.sql("""
            INSERT OVERWRITE TABLE app."""+ tableName + """ partition(dt='""" + today_date + """')
            SELECT 
                *
            FROM """ + temp_view + """
         """)

# 删除temp view
spark.catalog.dropTempView("temp_view")

# 行转列
# 为每个评论保留top 3相似的评论
comment_distance.createOrReplaceTempView('comment_distance')
sql = '''
-- 保留每个评论的top 3相似评论
WITH 
    comment_with_rank AS 
    (
        select 
            *,
            row_number() over (partition by ID1 order by Distance asc) Ranking
        from 
            comment_distance
    ),
-- 每个评论留1条详情
    comment_info AS 
    (
        select
            * 
        from 
            comment_with_rank
        where 
            Ranking=1
    ),
-- 每条评论top3拉平为列
    comment_with_top3 AS 
    (
        select 
            ID1,
            collect_set(ID2) Similar_IDs,
            collect_set(Movie_Name_CN2) Similar_Movie_Name_CNs,
            collect_set(Comment2) Similar_Comments
        from 
            comment_with_rank
        where 
            Ranking <= 3
        group by 
            ID1
    )
-- 输出结果
select
    a.ID1 ID,
    b.Movie_Name_CN1 Movie_Name_CN,
    b.Comment1 Comment,
    a.Similar_IDs,
    a.Similar_Movie_Name_CNs,
    a.Similar_Comments
from 
    comment_with_top3 a
left join 
    comment_info b
on 
    a.ID1=b.ID1
'''
similar_comment = spark.sql(sql)
similar_comment.show(truncate=False)


