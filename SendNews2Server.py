# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import random
import urllib2
import urllib
import time
import json
from help import Log

tagList = [u'新闻',
            u'军事',
            u'社会',
            u'国际',
            u'财经',
            u'股票',
            u'基金',
            u'外汇',
            u'科技',
            u'手机',
            u'探索',
            u'众测',
            u'体育',
            u'NBA',
            u'英超',
            u'中超',
            u'娱乐',
            u'明星',
            u'电影',
            u'星座',
            u'汽车']

timeList = ['20160101','20160102','20160103','20160104','20160105']

class Debug:
    def __init__(self):
        pass
    def debug(self, something2Print , isdebug = False , needLog = False):
        if isdebug:
            print something2Print
            time.sleep(10)
            
            
            
class GetNews:
    def __init__(self):
        '''initialize
        '''
        pass
    def getNewFromFile(self,folder , source = u'凤凰新闻'):
        '''red news from folder .the file name in folder is new's titile.
        the content in file is news.
        input:
        @foder : the folder content news file.
        output:
        @result : a dict . 
                 ex :[{title:***,tag:****,publishTime:***,from:***,\
                 content:**** , 'pv':***},*****]
                 tag: ex 'joy,military'
        '''
        debug = False
        result = []
        fileList = os.listdir(folder)
        i = 0
        for new_file in fileList:
            one_new = {}
            new_name = new_file.split('.')[0]
            one_new.setdefault('title',new_name)
            full_file = folder + '/'+new_file
            f = open(full_file)
            content = ''
            for line in f.readlines():
                content += line
            one_new.setdefault('content',content)
            tag = random.sample(tagList , 2)
            tag_s = ''
            for t in tag:
                tag_s += t+','
            one_new.setdefault('tag' , tag_s[:-1])
            one_new.setdefault('from' , source)
            one_new.setdefault('pv' , random.randint(10,10000))
            one_new.setdefault('publishTime' , random.sample(timeList,1))
            f.close()
            result.append(one_new)
            Debug().debug(one_new['content'],isdebug = debug)
            Debug().debug(one_new['title'],isdebug = debug)
            i += 1
            if i % 10 == 0:
                Log().write('Finish read %d news' % i)
        return result
    
class SendInfo2Server:
    def __init__(self):
        '''
        '''
        pass
    def sendByPost(self , dataList , url = '127.0.0.1' , port = 8888):
        '''send str(data) 
        input:
        @data:result of GetNews.getNewFromFile
        '''
        debug = False
        #When it tries to proxy "127.0.0.01/", the proxy gives up and returns a 504 error.
        #so need to setup an empty proxy
        proxy_build = urllib2.ProxyHandler({})
        opener = urllib2.build_opener(proxy_build)
        urllib2.install_opener(opener)        
        i = 0
        requrl = 'http://'+url+':'+str(port)+'/'
        Log().write("Begin send news to server")
        for d in dataList:            
            req = urllib2.Request(requrl)
            req.add_header('Content-Type', 'application/json')
            urllib2.urlopen(req, json.dumps(d))
            Debug().debug('http send' + d['title'] , isdebug = debug)
            i += 1
            if i % 10 == 0:
                Log().write('Already send %d news to server' % i)
        Log().write('send %d news to server' % i)
            
#test
GN = GetNews()
SS = SendInfo2Server()

folder = u'/home/hp/CODE/Recommend/data/凤凰新闻'
fh_news = GN.getNewFromFile(folder)
SS.sendByPost(fh_news)

folder = u'/home/hp/CODE/Recommend/data/新浪新闻'
xl_news = GN.getNewFromFile(folder)
SS.sendByPost(xl_news)
#endtest