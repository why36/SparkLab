#%%
import pandas as pd
import findspark
findspark.init() 
from pyspark import SparkContext
sc = SparkContext('local', 'test')

#%%
lines = sc.textFile('/home/why/lab4/data_format1/user_info_format1.csv')
header = lines.first()
lines = lines.filter(lambda row:row != header)
count = lines.map(lambda line: line.split(",")) \
        .map(lambda line: (line[1], 1)) \
        .reduceByKey(lambda a,b: a + b)

count.saveAsTextFile("result2_2")



