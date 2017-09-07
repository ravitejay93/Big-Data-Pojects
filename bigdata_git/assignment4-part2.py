# Databricks notebook source
import sys
import os

rawRatings = sc.textFile("/FileStore/tables/rul6s4dd1499701339696/ratings.dat").repartition(2)
rawUsers = sc.textFile("/FileStore/tables/13qdb6241499701357312/gender.dat")

# COMMAND ----------

def get_ratings_tuple(entry):
    """ Parse a line in the ratings dataset
    Args:
        entry (str): a line in the ratings dataset in the form of UserID,ProfileID,Rating
    Returns:
        tuple: (UserID, ProfileID, Rating)
    """
    items = entry.split(',')
    return int(items[0]), int(items[1]), float(items[2])


def get_user_tuple(entry):
    """ Parse a line in the movies dataset
    Args:
        entry (str): a line in the movies dataset in the form of UserID,Gender
    Returns:
        tuple: (UserID, Gender)
    """
    items = entry.split(',')    
    return int(items[0]), items[1]

#As the data set is large, running ALS crashes the server. Sampling the data to get a small portion  
ratingsRDD = rawRatings.map(get_ratings_tuple).sample(True,0.3).cache()
userRDD = rawUsers.map(get_user_tuple).cache()




# COMMAND ----------

#get top 10 profiles that are rated highest
def get_average(vec):
  
  sum = 0.0
  buff = list(vec[1])
  for val in buff:
    sum+=val
    
  return (vec[0],(len(buff),(sum*1.0)/len(buff)))


profileAverageWithCount = ratingsRDD.map(lambda x: (x[1],x[2])).groupByKey().map(get_average)
profileIdTop10 = profileAverageWithCount.filter(lambda x: x[1][0] > 50).sortBy(lambda x: str(x[1][1]) + ' ' + str(x[0]),False)

profileIdTop10.take(10)
  
  

# COMMAND ----------

#Divide the trainig set int 80:20 partitons
trainingRDD, validationRDD, testRDD = ratingsRDD.randomSplit([8,0,2])

print 'Training: %s, validation: %s, test: %s\n' % (trainingRDD.count(),
                                                    validationRDD.count(),
                                                    testRDD.count())

# COMMAND ----------

#Define Error function

import math

def computeError(predictedRDD, actualRDD):
    """ Compute the root mean squared error between predicted and actual
    Args:
        predictedRDD: predicted ratings for each movie and each user where each entry is in the form
                      (UserID, BookID, Rating)
        actualRDD: actual ratings where each entry is in the form (UserID, BookID, Rating)
    Returns:
        RSME (float): computed RSME value
    """
    # Transform predictedRDD into the tuples of the form ((UserID, MovieID), Rating)
    predictedReformattedRDD = predictedRDD.map(lambda x: ((x[0],x[1]),x[2])).cache()

    # Transform actualRDD into the tuples of the form ((UserID, MovieID), Rating)
    actualReformattedRDD = actualRDD.map(lambda x: ((x[0],x[1]),x[2])).cache()
    
    # Compute the squared error for each matching entry (i.e., the same (User ID, Movie ID) in each
    # RDD) in the reformatted RDDs using RDD transformtions - do not use collect()
    squaredErrorsRDD = (predictedReformattedRDD
                        .join(actualReformattedRDD).map(lambda x: (x[1][0] - x[1][1])*(x[1][0] - x[1][1])))

    # Compute the total squared error - do not use collect()
    totalError = squaredErrorsRDD.reduce(lambda a,b: a+b)

    # Count the number of entries for which you computed the total squared error
    numRatings = squaredErrorsRDD.count()

    # Using the total squared error and the number of entries, compute the RSME
    return math.sqrt(totalError*1.0/numRatings)
  

# COMMAND ----------

#get the best values for ALS
from pyspark.mllib.recommendation import ALS
testForPredictRDD = testRDD.map(lambda x: (x[0],x[1]))

seed = 5L
iterations = 5
regularizationParameter = 0.1
ranks = [3,4,5]
errors = [0, 0, 0]
err = 0
tolerance = 0.03

minError = float('inf')

bestRank = -1
bestIteration = -1
for rank in ranks:
    model = ALS.train(trainingRDD, rank, seed=seed, iterations=iterations,
                      lambda_=regularizationParameter)
    predictedRatingsRDD = model.predictAll(testForPredictRDD)
    error = computeError(predictedRatingsRDD, testRDD)
    errors[err] = error
    err += 1
    print 'For rank %s the RMSE is %s' % (rank, error)
    if error < minError:
        minError = error
        bestRank = rank

print 'The best model was trained with rank %s' % bestRank




# COMMAND ----------


