#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:09:48 2017

@author: hp
"""



from pyspark import SparkContext
from pyspark.sql import SQLContext
import datetime
from pyspark.sql.types import StructType , DoubleType , IntegerType , StringType , StructField
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from help import Log
from pyspark.sql import Row

sc = SparkContext("local[1]" , "RecommendServer")
sc.setLogLevel("ERROR")
sqc = SQLContext(sc)

#read rating data into spark
rating_file = '/home/hp/CODE/Recommend/data/rating.txt' # the structure of txt is userId , newsId , rating

ratings_df_schema = StructType(
        [StructField("userId" , IntegerType()),
         StructField("newsId" ,StringType()),
         StructField("rating" , DoubleType())
                ])
ratings_df = sqc.read.text(rating_file , header = True , schema = ratings_df_schema)
ratings_df.cache()

#train and choose model
seed = 1800009193L
(training_df , validation_df , test_df) = ratings_df.randomSplit([0.6 , 0.2 , 0.2] , seed)

als = ALS(maxIter = 20 , seed = seed , regParam = 0.1 , userCol = "userId" , itemCol = "newsId" , ratingCol = "rating")
als.setPredictionCol("prediction")

reg_eval = RegressionEvaluator(predictionCol = 'prediction' , labelCol = 'rating' , metricName = 'rmse')
ranks = [4, 8, 12]
errors = []
models = []
err = 0
min_err = float('inf')
best_rank = -1
for rank in ranks:
    als.setRank(rank)
    model = als.fit(training_df)
    predict_df = model.transform(validation_df)
    predict_ratings_df = predict_df.filter(predict_df.prediction != float('nan'))
    error = reg_eval(predict_ratings_df)
    Log().write("When training als model , for %d rank the rmse is %f" % (rank , error ))
    if error < min_err:
        min_err = error
        best_rank = err
        err += 1
    errors.append(error)
    models.append(model)
als.setRank(ranks[best_rank])
Log().write("The best model was traind with rank %d" % ranks[best_rank])
choosedModel = models[best_rank]


all_rating_model = als.fit(ratings_df)


