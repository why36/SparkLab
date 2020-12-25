#%%
import pandas as pd
import findspark
findspark.init() 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,IntegerType,DoubleType
from pyspark.sql.functions import udf, col

from pyspark.ml.linalg import Vector,Vectors
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql import Row,functions
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel,\
BinaryLogisticRegressionSummary,LogisticRegression,RandomForestClassifier

sc = SparkContext('local', 'test')
sqlContext = SQLContext(sc)

infoSchema = StructType([
   StructField("user_id", DoubleType(), True),
   StructField("age_range", DoubleType(), True),
   StructField("gender", DoubleType(), True)])

logSchema = StructType([
   StructField("user_id", DoubleType(), True),
   StructField("item_id", DoubleType(), True),
   StructField("cat_id", DoubleType(), True),
   StructField("seller_id", DoubleType(), True),
   StructField("brand_id", DoubleType(), True),
   StructField("time_stamp", DoubleType(), True),
   StructField("action_type", DoubleType(), True)])

trainSchema = StructType([
   StructField("user_id", DoubleType(), True),
   StructField("merchant_id", DoubleType(), True),
   StructField("label", DoubleType(), True)])

testSchema = StructType([
   StructField("user_id", DoubleType(), True),
   StructField("merchant_id", DoubleType(), True),
   StructField("prob", DoubleType(), True)])

user_info = sqlContext.read.format('csv').options(header='true').schema(infoSchema) \
        .load('/home/why/lab4/data_format1/user_info_format1.csv')
user_log = sqlContext.read.format('csv').options(header='true').schema(logSchema) \
        .load('/home/why/lab4/data_format1/user_log_format1.csv')
train = sqlContext.read.format('csv').options(header='true').schema(trainSchema) \
        .load('/home/why/lab4/data_format1/train_format1.csv')
test = sqlContext.read.format('csv').options(header='true').schema(testSchema) \
        .load('/home/why/lab4/data_format1/test_format1.csv')




#特征工程
# %%
train = train.join(user_info,on = "user_id",how = "left")

numLog = user_log.groupBy("user_id","seller_id").count() \
        .withColumnRenamed('seller_id', 'merchant_id') \
        .withColumnRenamed("count", "lognum")

train = train.join(numLog, on = ['user_id', 'merchant_id'], how = 'left')

# %%
actionLog = user_log.groupBy("user_id","seller_id","action_type").count() \
        .withColumnRenamed('seller_id', 'merchant_id') \
        .withColumnRenamed('count', 'times')
actionLog = actionLog.withColumn('click', ((actionLog["action_type"] == 0.0).cast("double")) * actionLog['times'])
actionLog = actionLog.withColumn('shoppingcart', ((actionLog["action_type"] == 1.0).cast("double")) * actionLog['times'])
actionLog = actionLog.withColumn('purchase', ((actionLog["action_type"] == 2.0).cast("double")) * actionLog['times'])
actionLog = actionLog.withColumn('favorite', ((actionLog["action_type"] == 3.0).cast("double")) * actionLog['times'])
actionLog = actionLog.groupBy('user_id','merchant_id').sum('click','shoppingcart','purchase','favorite') \
        .withColumnRenamed('sum(click)','click') \
        .withColumnRenamed('sum(shoppingcart)','shoppingcart') \
        .withColumnRenamed('sum(purchase)','purchase') \
        .withColumnRenamed('sum(favorite)','favorite')
#actionLog.show()
train = train.join(actionLog, on = ['user_id', 'merchant_id'], how = 'left')



itemLog = user_log.groupBy('user_id','seller_id','item_id').count().select('user_id','seller_id','item_id')
itemLog = itemLog.groupBy('user_id','seller_id').count() \
        .withColumnRenamed('seller_id', 'merchant_id') \
        .withColumnRenamed('count', 'itemNum')

train = train.join(itemLog,on = ['user_id', 'merchant_id'], how = 'left')
# %%
catLog = user_log.groupBy('user_id','seller_id','cat_id').count().select('user_id','seller_id','cat_id')
catLog = catLog.groupBy('user_id','seller_id').count() \
        .withColumnRenamed('seller_id', 'merchant_id') \
        .withColumnRenamed('count', 'catNum')

train = train.join(catLog,on = ['user_id', 'merchant_id'], how = 'left')

dayLog = user_log.groupBy('user_id','seller_id','time_stamp').count().select('user_id','seller_id','time_stamp')
dayLog = dayLog.groupBy('user_id','seller_id').count() \
        .withColumnRenamed('seller_id', 'merchant_id') \
        .withColumnRenamed('count', 'dayNum')

train = train.join(dayLog,on = ['user_id', 'merchant_id'], how = 'left')

# %%

train = train.na.replace(2.0, None, subset = 'gender')
train = train.na.replace(0.0, None, subset = 'age_range')
train = train.na.drop()

# %%

trainData, testData = train.randomSplit([0.8,0.2])

#weight
# num0 = trainData.filter(trainData['label']==0.0).count()
# trainsize = trainData.count()
# balancingRatio0 = trainsize / (2 * num0)
# balancingRatio1 = trainsize / (2 * (trainsize - num0))
#balancingRatio0 = 0.533
#balancingRatio1 = 8.076
# balancingRatio0 = 0.2
# balancingRatio1 = 1
# def getweight(label):
#     if label == 0.0:
#         return balancingRatio0
#     else:
#         return balancingRatio1

# getWeight = udf(getweight, returnType=DoubleType())
# trainData = trainData.withColumn("weight", getWeight(trainData['label']))
 

#%%
inputFeat = ['age_range', 'gender', 'lognum', 'click', 'shoppingcart', 'purchase', 'favorite']

df_assembler = VectorAssembler(inputCols=inputFeat, outputCol='features')

featureIndexer = VectorIndexer(maxCategories=8).setInputCol('features'). \
    setOutputCol('indexedFeatures')

#逻辑回归
lr = LogisticRegression(labelCol='label',featuresCol='indexedFeatures',\
                        maxIter=100, regParam=0.1)

#随机森林
rf = RandomForestClassifier(labelCol="label",featuresCol='indexedFeatures',subsamplingRate=0.382)

Pipeline = Pipeline().setStages([df_assembler, featureIndexer, lr])

PipelineModel = Pipeline.fit(trainData)

Predictions = PipelineModel.transform(testData)

# %%


acc_evaluator = MulticlassClassificationEvaluator(). \
    setLabelCol("label"). \
    setPredictionCol("prediction")
lrAccuracy = acc_evaluator.evaluate(Predictions)
print("Accuracy: %f " %lrAccuracy)

auc_evaluator = BinaryClassificationEvaluator(). \
    setLabelCol("label"). \
    setRawPredictionCol("rawPrediction")
auc = auc_evaluator.evaluate(Predictions)
print("AUC: %f " %auc)
# %%

Predictions.show()
# %%
test = test.join(user_info,on = "user_id",how = "left")
test = test.join(numLog, on = ['user_id', 'merchant_id'], how = 'left')
test = test.join(actionLog, on = ['user_id', 'merchant_id'], how = 'left')
test = test.join(itemLog,on = ['user_id', 'merchant_id'], how = 'left')
test = test.join(catLog,on = ['user_id', 'merchant_id'], how = 'left')
test = test.join(dayLog,on = ['user_id', 'merchant_id'], how = 'left')


test.coalesce(1).write.format('csv').option("header", "true").save('/home/why/lab4/data_format1/mytest')

# %%
