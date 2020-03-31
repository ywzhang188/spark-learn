#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *

rawDataWithHeader = sc.textFile('/test/train.tsv')
r_lines = rawDataWithHeader.map(lambda x: x.replace("\"", "")).map(lambda x: x.split("\t"))
header = r_lines.first()
lines = r_lines.filter(lambda x: x != header)
ds_raw = spark.createDataFrame(lines, header)

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import FloatType

to_float = udf(lambda x: 0 if x == "?" else float(x), FloatType())

from pyspark.sql.functions import col

ds = ds_raw.select([to_float(col(column)).alias(column) for column in ds_raw.columns[4:-1]])

# array
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType

determine_winner_udf = udf(lambda arr: arr[2] if arr[0] > arr[1] else arr[3], StringType())

df_with_winner = dropped_df.withColumn("winner", determine_winner_udf(
    array('homeFinalRuns', 'awayFinalRuns', 'homeTeamName', 'awayTeamName')))
display(df_with_winner)
