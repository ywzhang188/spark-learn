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

# word_count
df = spark.sparkContext.parallelize([(10100720363468236, ["what", "sad", "to", "me"]),
                                     (10100720363468236, ["what", "what", "does", "the"]),
                                     (10100718890699676, ["at", "the", "oecd", "with"])]).toDF(["id", "message"])
unpack_udf = udf(lambda l: [item for sublist in l for item in sublist])
from pyspark.sql.types import *
from collections import Counter

# We need to specify the schema of the return object
schema_count = ArrayType(StructType([
    StructField("word", StringType(), False),
    StructField("count", IntegerType(), False)
]))

count_udf = udf(
    lambda s: Counter(s).most_common(),
    schema_count
)
from pyspark.sql.functions import collect_list

(df.groupBy("id")
 .agg(collect_list("message").alias("message"))
 .withColumn("message", unpack_udf("message"))
 .withColumn("message", count_udf("message"))).show(truncate=False)

# groupby agg
from pyspark.sql import functions as F

a = sc.parallelize([[1, 1, 'a'],
                    [1, 2, 'a'],
                    [1, 1, 'b'],
                    [1, 2, 'b'],
                    [2, 1, 'c']]).toDF(['id', 'value1', 'value2'])
def find_a(x):
    """Count 'a's in list."""
    output_count = 0
    for i in x:
        if i == 'a':
          output_count += 1
    return output_count

find_a_udf = F.udf(find_a, IntegerType())
# count a in list
a.groupBy('id').agg(find_a_udf(F.collect_list('value2')).alias('a_count')).show()
# filter data if value1==1, then count a in list
a.groupBy('id').agg(find_a_udf(F.collect_list(F.when(F.col('value1') == 1, F.col('value2')))).alias('a_count')).show()

# 分词
def jieba_f(line):
    remove_chars_pattern = re.compile('[·’!"#$%&\'()＃！（）*+,-./:;<=>?@，：?★、…．＞【】［］《》？“”‘’[\\]^_`{|}~]+')
    try:
        words = [remove_chars_pattern.sub('', word) for word in jieba.lcut(line, cut_all=False)]
        return words
    except:
        return []
jieba_udf = udf(jieba_f, ArrayType(StringType()))
douban_df = douban_df.withColumn('Words', jieba_udf(col('Comment')))
