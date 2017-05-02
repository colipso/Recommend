#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:09:48 2017

@author: hp
"""



from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType , DoubleType , IntegerType , StringType , StructField
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from help import Log
from pyspark.sql import Row
from pyspark.sql.functions import col
import tornado.ioloop
import tornado.web


sc = SparkContext("local[1]" , "RecommendServer")
sc.setLogLevel("ERROR")
sqc = SQLContext(sc)

#read rating data into spark
rating_file = '/home/hp/CODE/Recommend/data/sparkActions.txt' # the structure of txt is userId , newsId , rating


ratings_df = sqc.read.json(rating_file)
ratings_df.cache()

#train and choose model
seed = 1800009193L
(training_df , validation_df , test_df) = ratings_df.randomSplit([0.5 , 0.2 , 0.3] , seed)

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
    error = reg_eval.evaluate(predict_ratings_df)
    Log().write("When training als model , for %d rank the rmse is %f" % (rank , error ))
    if error < min_err:
        min_err = error
        best_rank = err
        err += 1
    errors.append(error)
    models.append(model)
als.setRank(ranks[best_rank])
Log().write("The best model was traind with rank %d" % ranks[best_rank])

all_rating_model = als.fit(ratings_df)
predict_df = all_rating_model.transform(ratings_df)
predicted_user_df = predict_df.filter(col('prediction') != float('nan'))


def recommendByCF(userId):
    '''recommend by trained result df predicted_user_df
    output:
        a list of newsId
    '''
    user_rated_newsId = ratings_df.filter(col('userId') == userId).select(col('newsId')).rdd.flatMap(lambda x:x).collect()
    print user_rated_newsId
    if len(user_rated_newsId) == 0:
        return False
    predicted_not_rated_df = predicted_user_df.filter( ~predicted_user_df.newsId.isin(user_rated_newsId))
    predicted_highest_not_rated_df = predicted_not_rated_df.sort(predicted_not_rated_df.prediction ,ascending = False)
    
    predicted_highest_not_rated_df.show(20,truncate = False)
    
    recommendNewsList = predicted_highest_not_rated_df.select(col('newsId')).rdd.flatMap(lambda x:x).collect()
    return recommendNewsList
    


#test
#userId = 276729
#recommendByCF(userId)
#endtest

news_file = '/home/hp/CODE/Recommend/data/sparkData.txt' # the structure of txt is userId , newsId , rating





class SendUserInfoHandler(tornado.web.RequestHandler):
    def post(self):
        '''@data {userId:4613,userTags:aaa,bbb,ccc}
        '''
        data = tornado.escape.json_decode(self.request.body)
        userId = data['userId']
        userTags = data['userTags'].split(',')
        recommendListByCF = recommendByCF(userId)
        if recommendListByCF != False:
            self.write(recommendListByCF)
        #debug
        print data
        print userTags
        #enddebug
        
    def get(self):
        '''get url
        http://127.0.0.1:8886/senduserinfo?userid=276729&tag=military
        '''
        userId = self.get_argument('userid')
        recommendListByCF = recommendByCF(userId)
        if recommendListByCF != False:
            self.write(recommendListByCF)


def make_app():
    return tornado.web.Application([
        (r"/senduserinfo", SendUserInfoHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    port = 8886
    app.listen(port)
    Log().write('Save action server Begin to listen port %d' % port)
    tornado.ioloop.IOLoop.current().start()
    

