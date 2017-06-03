#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 14:11:37 2017

@author: hp
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 10:57:30 2017

@author: hp
"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import tornado.ioloop
import tornado.web
import time
import json
import os
from help import Log


fileName = os.getcwd() + '/data/dparkActions.txt'

structureList = ['userId','newsId','rating']

class SaveData2File:
    def __init__(self):
        '''
        '''
        pass
    def append(self , onelineData , fileName = fileName , structureList = structureList):
        '''append data to file
        input:
            @onelineData: json like txt .ex:'{"userId": 1, "newsId": 2,"rating":10 }'
        '''
        f = open(fileName , 'a')
        data = json.dumps(onelineData)
        if isinstance(onelineData , dict):
            if 'userId' in onelineData.keys():
                Log().write(u'user %s read new %s' % (str(onelineData['userId']) ,str(onelineData['newsId'])))
        f.write(data)
        f.write('\n')
        f.close()

class SendActionsHandler(tornado.web.RequestHandler):
    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        SaveData2File().append(data)
        
    def get(self):
        '''get url
        http://127.0.0.1:8888?name=hp
        '''
        self.set_cookie('username', 'peng', expires=time.time()+900)
        nowamagic = self.get_argument('name')
        print nowamagic
       

def make_app():
    return tornado.web.Application([
        (r"/sendactions", SendActionsHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    port = 8887
    app.listen(port)
    Log().write('Save action server Begin to listen port %d' % port)
    tornado.ioloop.IOLoop.current().start()