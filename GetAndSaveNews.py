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
from help import Log
import json

fileName = '/home/hp/CODE/Recommend/data/sparkData.txt'

class SaveData2File:
    def __init__(self):
        '''
        '''
        pass
    def append(self , onelineData , fileName = fileName):
        '''append data to file
        input:
            @onelineData: json like txt .ex:'{"a": 1, "b": 2}'
        '''
        f = open(fileName , 'a')
        f.write(json.dumps(onelineData))
        f.write('\n')
        f.close()

class MainHandler(tornado.web.RequestHandler):
    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        time.sleep(60)
        SaveData2File().append(data)
        
    def get(self):
        '''get url
        url example:http://127.0.0.1:8888?name=hp
        '''
        self.set_cookie('username', 'peng', expires=time.time()+900)
        nowamagic = self.get_argument('name')
        print nowamagic
       

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    port = 8888
    app.listen(port)
    Log().write('Begin to listen port %d' % port)
    tornado.ioloop.IOLoop.current().start()
    