# Databricks notebook source
import os
import sys
from pyspark.mllib.clustering import KMeans, KMeansModel


def todouble(line):  
  result = []
  i = 0
  while i < len(line):
    result.append(float(line[i]))
    i+=1
  return result

#take seed data and convert to double
trainingRaw = sc.textFile("/FileStore/tables/ghwlpxtt1499907037815/seeds.txt")
trainingData = trainingRaw.map(lambda x: x.split('\t')).map(todouble)

trainingData.collect()


# COMMAND ----------

maxClus = [2,5,7,10,20,40,60,100,200,400]
least = sys.maxint 
c = 0
for val in maxClus:
  clusters = KMeans.train(trainingData, val, maxIterations=20, initializationMode="random")

  WSSSE = clusters.computeCost(trainingData)
  if(least > WSSSE):
    least = WSSSE
    c = val
  
  print("Within Set Sum of Squared Error for "+ str(val) +" is " + str(WSSSE))

print("least WSSSE is " + str(least) + " cluster size of " + str(c))

# COMMAND ----------


