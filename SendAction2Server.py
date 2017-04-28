#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 15:11:09 2017

@author: hp
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


import urllib2
import time
import json
from help import Log
import MySQLdb



class Debug:
    def __init__(self):
        pass
    def debug(self, something2Print , isdebug = False , needLog = False):
        if isdebug:
            print something2Print
            time.sleep(10)
            
            
            
class GetActions:
    def __init__(self , host = 'localhost' , port = 3306 , user = 'root' , passwd = '254449597' , db = 'news'):
        '''initialize
        
        '''
        conn = MySQLdb.connect(host = host , port = port , user = user , passwd = passwd , db = db)
        self.cur = conn.cursor()
    def getActionFromDB(self , table = 'actions'):
        '''get action from mysqldb
        
        @result : a list of dict . 
                 ex :[{userId:***,newsId:****,rating:***},*****]
        '''
        self.cur.execute("select * from %s;" % table)
        result = self.cur.fetchall()
        returnData = []
        for r in result:
            oneAction = {}
            oneAction.setdefault('userId',r[0])
            oneAction.setdefault('newsId',r[1])
            oneAction.setdefault('rating',r[2])
            returnData.append(oneAction)
        return returnData

    
class SendInfo2Server:
    def __init__(self):
        '''
        '''
        pass
    def sendByPost(self , dataList , url = '127.0.0.1' , port = 8887):
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
        requrl = 'http://'+url+':'+str(port)+'/sendactions'
        Log().write("Begin send news to server")
        for d in dataList:            
            req = urllib2.Request(requrl)
            req.add_header('Content-Type', 'application/json')
            urllib2.urlopen(req, json.dumps(d))
            i += 1
            if i % 10 == 0:
                Log().write('Already send %d actions to server' % i)
        Log().write('send %d actions to server' % i)
            
#test
GN = GetActions()
SS = SendInfo2Server()

actions = GN.getActionFromDB()
SS.sendByPost(actions)

