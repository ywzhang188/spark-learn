#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

# slice from array
df = spark.createDataFrame([([1, 2, 3], 'val1'), ([4, 5, 6], 'val2')], ['col1', 'col2'])
df.show()
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd
import pyspark.sql.functions as F


# method1
@pandas_udf(ArrayType(LongType()))
def func(v):
    res = []
    for row in v:
        res.append(row[1:])
    return pd.Series(res)


# method 2
@pandas_udf(ArrayType(LongType()))
def func(v):
    return v.apply(lambda x: x[1:])


df.withColumn('col3', func(df.col1)).show()

# create mew column with apply func
df = spark.createDataFrame(
    [('1', 1.0), ('1', 2.0), ('2', 2.0), ('2', 3.0), ('3', 3.0)],
    ("id", "value"))
schema = StructType(
    [StructField('id', StringType()), StructField('value', DoubleType()), StructField('value2', DoubleType())])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def subtract_mean(pdf):
    return pdf.assign(value2=pdf.value - pdf.value.mean())
# Input/output are both a pandas.DataFrame

df.groupby('id').apply(subtract_mean).take(2)

# filter col A value while col B max, for each unique in col C
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *

df = spark.createDataFrame(
    [(1, 1.0, 1), (1, 2.0, 0), (2, 3.0, 1), (2, 5.0, 0), (2, 10.0, 1)],
    ("id", "v", 'label'))

schema = StructType([StructField('id', LongType()), StructField('v', DoubleType()), StructField('label', LongType())])


# @pandas_udf("id long, v double, label long", PandasUDFType.GROUPED_MAP)
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def max_v_label(pdf):
    index_of_row = pdf["v"].idxmax()
    return pdf.iloc[
           index_of_row:index_of_row + 1]  # stupid hack to return a df and not a series, almost certanly a better way


display(df.groupby("id").apply(max_v_label))

# groupby key, calculate mean value of rows of min([A, B]), method 1
schema = StructType([
    StructField("key", StringType()),
    StructField("avg_minOfValue1AndValue2", DoubleType())
])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def g(df):
    result = pd.DataFrame(df.groupby("key").apply(
        lambda x: x.loc[:, ["value1", "value2"]].min(axis=1).mean()
    ), columns=['avg_minOfValue1AndValue2'])
    result.reset_index(inplace=True, drop=False)
    return result
df.groupby("key").apply(g).show()

# groupby key, calculate mean value of rows of min([A, B]), method 2
import numpy as np

@pandas_udf(DoubleType(), functionType=PandasUDFType.GROUPED_AGG)
def f(x, y):
    return np.minimum(x, y).mean()
df.groupBy("key").agg(f("value1", "value2").alias("avg_min")).show()


# process list

# load dictionary_json.json from https://github.com/dwyl/english-words
all_eng_words = {}


@pandas_udf(ArrayType(StringType()))
def _non_english_keywords_filter(v):
    # Join a column of english of dictionary words, thinking a different list of english words to the word vectors
    return v.apply(lambda x: list(filter(lambda w: w not in all_eng_words, x)))


# test
df = spark.createDataFrame([("http://ru.osvita.ua/vnz/guide/202", ["202", "vnz", "guide"]),
                            ("https://www1.kisscartoon.xyz/episode/the-amazing-world-of-gumball-season-5-episode-32",
                             ["amazing", "world", "gumball", "season", "episode", "32"])], ['url', 'url_keywords'])
df = df.limit(10).withColumn("non_eng_keywords",
                             _non_english_keywords_filter(df.url_keywords))

# apply func to row and return multiple columns

schema = StructType(
    [StructField('scores', ArrayType(FloatType())), StructField('category_ids', ArrayType(StringType()))])


def my_func(pdf, N):
    all_categories = pdf.columns[:-1]
    pdf = pdf.withColumn("scores", F.array(all_categories)).drop(*all_categories)
    schema = StructType(
        [StructField('scores', ArrayType(FloatType())), StructField('category_ids', ArrayType(StringType()))])

    @pandas_udf(schema)
    def n_scores_categories(pdf):
        result_df = pdf.apply(lambda row: pd.Series({'scores': list(row[np.argpartition(np.negative(row), N)][:N]),
                                                     'category_ids': list(
                                                         np.array(all_categories)[np.argpartition(np.negative(row), N)][
                                                         :N])}))
        return result_df

    new_df = pdf.withColumn("Output", F.explode(F.array(n_scores_categories(pdf['scores']))))
    new_df = new_df.select("word", "Output.*")
    return new_df


display(my_func(df_0, 3))

# median
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "price")
)

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def median_udf(v):
    return v.median()

df.groupBy("id").agg(median_udf(df["price"])).show()

# groupBy agg median of multiple columns
@pandas_udf(FloatType(), functionType=PandasUDFType.GROUPED_AGG)
def _func_median(v):
    return v.median()
expr_median = [_func_median(df[col]).alias(col+'_median') for col in numeric_features]
df_median = df.groupBy('cluster').agg(*expr_median).toPandas()
