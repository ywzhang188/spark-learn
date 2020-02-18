#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from spark_sql.getting_started.spark_session import *

textFile = spark.read.text("file:///root/apps/spark-2.4.4-bin-hadoop2.7/README.md")
textFile.count()
textFile.first()