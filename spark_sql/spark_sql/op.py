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

