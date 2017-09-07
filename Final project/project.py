# Databricks notebook source
import sys
import os
from pyspark.ml.feature import *
from pyspark.ml.evaluation import *
from pyspark.mllib.evaluation import *

from pyspark.mllib.recommendation import ALS

# get the data from the tmp file generated
# replace the file location with the file location in which the tmp file is saved
# rawSampled = sc.textFile(<Tmp File Location>).cache()

rawSampled = sc.textFile("/FileStore/tables/xi8s0vex1500739277781/rec_log_train_tmp.txt").cache()


# COMMAND ----------


#data of format (UserId)\t(ItemId)\t(Result)\t(Unix-timestamp)
#generate tuple

def get_tuple(data):
  
  item = data.split('\t')
  return int(item[0]),int(item[1]),float(item[2])
                  

testRDD = rawSampled.map(get_tuple).cache()


# COMMAND ----------

#split data int0 training and test

trainingRDD, testRDD = testRDD.randomSplit([0.8,0.2])

print 'Training: %s, test: %s\n' % (trainingRDD.count(),testRDD.count())

# COMMAND ----------

# convert the outputs to 1 or -1
import math

def generalize(data):
  val = 0.0
  if(data[2] >= 0.0):
    val = 1.0
  else:
    val = -1.0
  return (data[0],data[1],val)

# COMMAND ----------

# trian the model
testForPredictRDD = testRDD.map(lambda x: (x[0],x[1]))

seed = 5L
iterations = 5
regularizationParameter = 0.1
rank = 3
tolerance = 0.03

model = ALS.train(trainingRDD, rank, seed=seed, iterations=iterations,
                      lambda_=regularizationParameter)
predictedRatingsRDD = model.predictAll(testForPredictRDD).map(generalize)

# COMMAND ----------

# predict the values and test the model

predictedReformattedRDD = predictedRatingsRDD.map(lambda x: ((x[0],x[1]),x[2])).cache()
actualReformattedRDD = testRDD.map(lambda x: ((x[0],x[1]),x[2])).cache()
squaredErrorsRDD = predictedReformattedRDD.join(actualReformattedRDD).map(lambda x: [x[1][0],x[1][1]])
squaredDF = spark.createDataFrame(squaredErrorsRDD,["predicted","actual"])
evaluator = MulticlassClassificationEvaluator(labelCol="actual", predictionCol="predicted", metricName="accuracy")
accuracy = evaluator.evaluate(squaredDF)
squaredDF.describe("actual","predicted").show()

print accuracy

# COMMAND ----------

# convert the output into the desired plot
display(squaredDF)

# COMMAND ----------


