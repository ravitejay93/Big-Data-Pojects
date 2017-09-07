# Databricks notebook source
import sys
import os
from pyspark.ml.feature import *
from pyspark.ml.linalg import *
from pyspark.ml.classification import *
from pyspark.ml.regression import *
from pyspark.ml.evaluation import *
from pyspark.ml import Pipeline
from pyspark.mllib.linalg.distributed import *
from pyspark.mllib.evaluation import *
  
trainingSet = sc.textFile("/FileStore/tables/24uihk221499888162859/iris.txt")

trainingRDD = trainingSet.map(lambda x: x.split(',')).map(lambda x:[Vectors.dense(float(x[0]),float(x[1]),float(x[2]),float(x[3])),x[4]])

trainingDF = spark.createDataFrame(trainingRDD,["features","value"])

#converting the name of iris into index
indexer = StringIndexer(inputCol="value", outputCol="label")
model_index = indexer.fit(trainingDF)
trainingDF = model_index.transform(trainingDF)

#principle component ananlysis
pca = PCA(k=3, inputCol="features", outputCol="pca")
model_pca = pca.fit(trainingDF)
transformed = model_pca.transform(trainingDF)

#split data
(trainingData, testData) = transformed.randomSplit([0.8, 0.2])


#decission tree and random forest classifiers
dc_1 = DecisionTreeClassifier(labelCol="label", featuresCol="features")
rf_1 = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=5)
dc_2 = DecisionTreeClassifier(labelCol="label", featuresCol="pca")
rf_2 = RandomForestClassifier(labelCol="label", featuresCol="pca", numTrees=5)

#initialize evaluator
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

model = dc_1.fit(trainingData)
predictions = model.transform(testData)
accuracy = evaluator.evaluate(predictions)

testMetric = predictions.rdd.map(lambda x:(x.prediction,x.label))
metric = MulticlassMetrics(testMetric)

print("Decision Tree Classifier: %g " % accuracy + " Weighted precission:" + str(metric.weightedPrecision) + " Weighted F Measure:" + str(metric.weightedFMeasure()))


model_1 = rf_1.fit(trainingData)
predictions = model_1.transform(testData)
accuracy = evaluator.evaluate(predictions)

testMetric = predictions.rdd.map(lambda x:(x.prediction,x.label))
metric = MulticlassMetrics(testMetric)

print("Random Forest Classifier: %g" % accuracy+ " Weighted precission:" + str(metric.weightedPrecision) + " Weighted F Measure:" + str(metric.weightedFMeasure()))

model_2 = dc_2.fit(trainingData)
predictions = model_2.transform(testData)
accuracy = evaluator.evaluate(predictions)

testMetric = predictions.rdd.map(lambda x:(x.prediction,x.label))
metric = MulticlassMetrics(testMetric)

print("PCA & Decission Tree Classifier: %g" % accuracy+ " Weighted precission:" + str(metric.weightedPrecision) + " Weighted F Measure:" + str(metric.weightedFMeasure()))

model_3 = rf_2.fit(trainingData)
predictions = model_3.transform(testData)
accuracy = evaluator.evaluate(predictions)

testMetric = predictions.rdd.map(lambda x:(x.prediction,x.label))
metric = MulticlassMetrics(testMetric)

print("PCA & Random Forest Classifier: %g" % accuracy+ " Weighted precission:" + str(metric.weightedPrecision) + " Weighted F Measure:" + str(metric.weightedFMeasure()))

# COMMAND ----------


