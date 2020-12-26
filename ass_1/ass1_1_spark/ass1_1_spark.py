#%%
import findspark
findspark.init() 
from pyspark import SparkContext
sc = SparkContext('local', 'test')

lines = sc.textFile('/home/why/lab4/data_format1/user_log_format1.csv')
header = lines.first()#第一行 print(header)
lines = lines.filter(lambda row:row != header)#删除第一行


#%%
count = lines.map(lambda line: line.split(",")) \
        .map(lambda key: (key[0] + "#" + key[1] + "#" + key[6],key)) \
        .reduceByKey(lambda x,y: x) \
        .map(lambda x: x[1]) \
        .filter(lambda line: line[-2] == "1111") \
        .filter(lambda line: line[-1] != "0") \
        .map(lambda key: (key[1], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1],ascending=False) \
        .zipWithIndex().map(lambda x: (x[1],x[0][0],x[0][1])) \
        .take(100)

#print(count)
#%%
f=open("result1_1.txt","w")
list_line = [list(item) for item in count]
for line in list_line:
    s = str(line[0]) + ": " + line[1] + ",\t" + str(line[2])
    f.write(s+'\n')
f.close()


