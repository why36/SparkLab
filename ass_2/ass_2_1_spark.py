#%%
import pandas as pd
import findspark
findspark.init() 
from pyspark import SparkContext
sc = SparkContext('local', 'test')

lines = sc.textFile('/home/why/lab4/data_format1/user_log_format1.csv')
header = lines.first()#第一行 print(header)
lines = lines.filter(lambda row:row != header)#删除第一行

#%%
count = lines.map(lambda line: line.split(",")) \
        .filter(lambda line: line[-2] == "1111") \
        .filter(lambda key: key[-1] == "2") \
        .map(lambda key: (key[0] + "#" + key[1] + "#" + key[6],key)) \
        .reduceByKey(lambda x,y: x) \
        .map(lambda x: x[1])

tmp = count.collect()
users = [line[0] for line in tmp]
users_map = dict.fromkeys(users, 1)

#%%
lines = sc.textFile('/home/why/lab4/data_format1/user_info_format1.csv')
header = lines.first()
lines = lines.filter(lambda row:row != header)
count = lines.map(lambda line: line.split(",")) \
        .map(lambda line: (line[2], 1)) \
        .reduceByKey(lambda a,b: a + b)

count.saveAsTextFile("result2_1")



