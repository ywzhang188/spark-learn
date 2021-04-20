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
