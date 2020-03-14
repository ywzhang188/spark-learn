#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__='yzhang'

from getting_started.spark_session import *
import pandas as pd
import numpy as np

d = {'Id':[1,2,3,4,5,6],
     'Score': [4.00, 4.00, 3.85, 3.65, 3.65, 3.50]}
data = pd.DataFrame(d)
ds = spark.createDataFrame(data)

ds.select('Score').describe().show()
def describe_pd(df_in, columns, deciles=False):
    if deciles:
        percentiles = np.array(range(0, 110, 10))
    else:
        percentiles = [25, 50, 75]
    percs = np.transpose([np.percentile(df_in.select(x).collect(),percentiles) for x in columns])
    percs = pd.DataFrame(percs, columns=columns)
    percs['summary'] = [str(p) + '%' for p in percentiles]
    spark_describe = df_in.describe().toPandas()
    new_df = pd.concat([spark_describe, percs],ignore_index=True)
    new_df = new_df.round(2)
    return new_df[['summary'] + columns]
describe_pd(ds, ['Score'])

# skewness and kurtosis
from pyspark.sql.functions import skewness, kurtosis
var = 'Score'
ds.select(skewness(var), kurtosis(var)).show()

# histogram
import matplotlib.pyplot as plt
var = 'Score'
plot_data = ds.select(var).toPandas()
x = plot_data[var]
bins = [0, 3.6, 3.8, 3.9, 4]
hist, bin_edges = np.histogram(x,bins,weights=np.zeros_like(x) + 100. / x.size) # make the histogram
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(1, 1, 1)
# Plot the histogram heights against integers on the x axis
ax.bar(range(len(hist)),hist,width=1,alpha=0.8,ec ='black',color = 'gold')
# # Set the ticks to the middle of the bars
ax.set_xticks([0.5+i for i,j in enumerate(hist)])
# Set the xticklabels to a string that tells us what the bin edges were
#labels =['{}k'.format(int(bins[i+1]/1000)) for i,j in enumerate(hist)]
labels =['{}'.format(bins[i+1]) for i,j in enumerate(hist)]
labels.insert(0,'0')
ax.set_xticklabels(labels)
#plt.text(-0.6, -1.4,'0')
plt.xlabel(var)
plt.ylabel('percentage')
plt.savefig('./test.png')

# boxplot, violin plot
import seaborn as sns
fig = plt.figure(figsize=(20, 8))
ax = fig.add_subplot(1, 2, 1)
ax = sns.boxplot(data=plot_data)

ax =fig.add_subplot(1, 2, 2)
ax = sns.violinplot(data = plot_data)
plt.savefig('./boxplot_and_violinplot.png')

# check data null values
from pyspark.sql.functions import count


def my_count(ds):
    ds.agg(*[count(c).alias(c) for c in ds.columns]).show()
