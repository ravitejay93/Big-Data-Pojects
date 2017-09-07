# Databricks notebook source
import sys
import os
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Word2Vec
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# get the tmp data and convert to a tuple
# replace the file location with the file location in which the tmp file is saved
# rawData = sc.textFile(<Tmp File Location>).cache()

rawData = sc.textFile("/FileStore/tables/xi8s0vex1500739277781/rec_log_train_tmp.txt").cache()

rawSampled = rawData.sample(False,1).cache()
def get_tuple(data):
  
  item = data.split('\t')
  if(float(item[2]) == -1):
    return item[0],item[1], 0.0
  else:
    return item[0],item[1],1.0
                  
testRDD = rawSampled.map(get_tuple).cache()
testDF = spark.createDataFrame(testRDD, ["user","product","label"])
testDF.describe("label").show()

# COMMAND ----------

# get the data from user_key_word file and generate feature vectors

def convert_data(data):
  values = data.split(';')
  vec = map(lambda x: x.split(':'),values)
  buff = sorted(vec,key=lambda x: int(x[0]))
  result = ''
  for x in buff:
    result += x[0]+':'
    
  result = result[:-1]
  return result

# upload the user_key_word.txt and replace the <file location> with the new location
# userFeatureRDD = sc.textFile(<file location>).cache()

userFeatureRDD = sc.textFile("/FileStore/tables/dv4ow79y1501801226776/user_key_word.txt").cache()
userLibSvmRDD = userFeatureRDD.map(lambda x: x.split('\t')).map(lambda x: x[0]+' '+convert_data(x[1]))
inp = userLibSvmRDD.map(lambda x: x.split(' ')).map(lambda x: (x[0],x[1].split(':')))
inpDF = spark.createDataFrame(inp, ["user","input"])
word2vec = Word2Vec(vectorSize=5, minCount=0, inputCol="input", outputCol="result")
model = word2vec.fit(inpDF)
result = model.transform(inpDF)

# COMMAND ----------

# get the item keywords form item.txt and generate feature vectors
# upload the file item.txt and replace the <file location> with the new file location
# itemRawRDD = sc.textFile(<file location>)
itemRawRDD = sc.textFile("/FileStore/tables/7ahtri9r1501802021762/item.txt")
itemTmpRDD = itemRawRDD.map(lambda x: x.split('\t')).map(lambda x: (x[0],x[2].split(';')))
itemTmpDF = spark.createDataFrame(itemTmpRDD, ["product","input"])
word2vec = Word2Vec(vectorSize=5, minCount=0, inputCol="input", outputCol="categories")
model = word2vec.fit(itemTmpDF)
itemDF = model.transform(itemTmpDF)

# COMMAND ----------

# combine all features and generate a final feature vector comprising of user and item features

trainDF = testDF.join(itemDF, itemDF.product == testDF.product)
outDF = trainDF.join(result,result.user == trainDF.user)
assembler = VectorAssembler(inputCols=["categories", "result"],outputCol="features")

output = assembler.transform(outDF)

# COMMAND ----------

#train data

(trainingData, testData) = output.randomSplit([0.8, 0.2])
print 'Training: %s, test: %s\n' % (trainingData.count(),testData.count())

gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
pipeline = Pipeline(stages=[gbt])
model = pipeline.fit(trainingData)

# COMMAND ----------

#make prediction on test data

predictions = model.transform(testData)
predictions.describe("prediction","label").show()

# COMMAND ----------

graph = predictions[["prediction","label"]]

#convert the output into desired plot
display(graph)

# COMMAND ----------

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(graph)
print("Test Accuracy = %g" % accuracy)

# COMMAND ----------


