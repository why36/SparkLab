#%%
import pandas as pd
import findspark
findspark.init() 
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext('local', 'test')
sqlContext = SQLContext(sc)

info = sqlContext.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .load('/home/why/lab4/data_format1/user_info_format1.csv')

info.groupby('gender').count().show()
info.groupby('age_range').count().show()






# %%
