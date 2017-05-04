#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:09:48 2017

@author: hp
"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


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
predicted_user_df.cache()

def recommendByCF(userId):
    '''recommend by trained result df predicted_user_df
    userId:
    output:
        a list of newsId
    '''
    try:
        user_rated_newsId = ratings_df.filter(col('userId') == userId).select(col('newsId')).rdd.flatMap(lambda x:x).collect()
        print user_rated_newsId
        if len(user_rated_newsId) == 0:
            return False
        predicted_not_rated_df = predicted_user_df.filter( ~predicted_user_df.newsId.isin(user_rated_newsId))
        predicted_not_rated_df.cache()
        predicted_highest_not_rated_df = predicted_not_rated_df.sort(predicted_not_rated_df.prediction ,ascending = False)
        predicted_highest_not_rated_df.show(20,truncate = False)
        predicted_highest_not_rated_df.cache()
        recommendNewsList = predicted_highest_not_rated_df.select(col('newsId')).rdd.flatMap(lambda x:x).take(1000)
        Log().write("Recommend to user %d by CF success.Recommend %d news" % (userId , len(recommendNewsList) ))
        return recommendNewsList
    except Exception as e:
        Log().write("Something wrong when recommend to user(%d) by CF.Err info : %s" % (userId,e),"ERROR")
        return False
    


'''
Recommend by tag. Give user the hotest news in his tag which not readed.
'''

news_file = '/home/hp/CODE/Recommend/data/sparkData.txt' # the structure of txt is userId , newsId , rating
news_df = sqc.read.json(news_file) #{title:***,tag:****,publishTime:***,from:***,content:**** , 'pv':*** , id:*****}
news_df.cache()
sorted_news_df = news_df.sort(news_df.pv.desc(),news_df.publishTime.desc())

def recommendByTag(userId,userTagList = [u'娱乐',u'新闻']):
    '''recommend by the news which is in the tag that user readed and which user not readed
    if a user did not read a new that already pushed . the action rating is 0
    output:
        a dict with key is tag and values are list of news id.
        ex:{'fun':[1,2,3] , 'phone':[5,6,7]}
    '''
    try:
        user_readed_newsId = ratings_df.filter(col('userId') == userId).filter(col('rating') > 0).select(col('newsId')).rdd.flatMap(lambda x:x).collect()
        if len(user_readed_newsId) > 0:
            user_not_readed_df = sorted_news_df.filter( ~sorted_news_df.id.isin(user_readed_newsId))
        else:
            user_not_readed_df = sorted_news_df
        user_not_readed_df.cache()
        if len(userTagList) == 0:
            return False
        returnDict = {}
        i = 0
        for tag in userTagList:
            recommendListByTag = user_not_readed_df.filter(col('tag').like(u'%%%s%%' % tag)).select('id').rdd.flatMap(lambda x:x).take(500)
            returnDict.setdefault(tag,recommendListByTag)
            i += len(recommendListByTag)
        Log().write("Recommend to user %d by tag %s success. Recommend %d news" % (userId , userTagList ,i))
        
        return returnDict
    except Exception as e:
        Log().write("Something wrong when recommend to user(%d) by tag(%s).Err info : %s" % (userId,str(userTagList),e),"ERROR")
        return False

def h_merge2List(list1 , list2):
    num = min(len(list1), len(list2))
    result = [None]*(num*2)
    result[::2] = list1[:num]
    result[1::2] = list2[:num]
    result.extend(list1[num:])
    result.extend(list2[num:])
    return result

class SendUserInfoHandler(tornado.web.RequestHandler):
    def post(self):
        '''@data {userId:4613,userTags:aaa,bbb,ccc}
        output:
            ex : {'recommend':[1,2,3,4]}
        '''
        data = tornado.escape.json_decode(self.request.body)
        userId = data['userId']
        userId = int(userId)
        userTags = data['userTags'].split(',')
        recommendListByCF = recommendByCF(userId)
        recommendDicByTag = recommendByTag(userId , userTags)
        returnList = []
        if recommendDicByTag != False:
            for tag in recommendDicByTag.keys():
                returnList = h_merge2List(returnList , recommendDicByTag[tag])
        if recommendListByCF != False:
            returnList = h_merge2List(returnList , recommendListByCF)
        
        recommendDic = {'recommend':returnList}
            
        self.write(recommendDic)

        
    def get(self):
        '''get url
        http://127.0.0.1:8886/getrecommend?userid=276729&tag=娱乐,探索
        output:
            ex : {'recommend':[1,2,3,4]}
        '''
        userId = self.get_argument('userid')
        userId = int(userId)
        userTags = self.get_argument('tag').split(',')
        recommendListByCF = recommendByCF(userId)
        recommendDicByTag = recommendByTag(userId , userTags)
        returnList = []
        if recommendDicByTag != False:
            for tag in recommendDicByTag.keys():
                returnList = h_merge2List(returnList , recommendDicByTag[tag])
        if recommendListByCF != False:
            returnList = h_merge2List(returnList , recommendListByCF)
        
        recommendDic = {'recommend':returnList}
        self.write(recommendDic)


def make_app():
    return tornado.web.Application([
        (r"/getrecommend", SendUserInfoHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    port = 8886
    app.listen(port)
    Log().write('News recommend server Begin to listen port %d' % port)
    tornado.ioloop.IOLoop.current().start()
    

