#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 27 16:09:33 2017

@author: hp
"""
import time

class Log:
    def __init__(self,logPath = '/home/hp/CODE/Recommend/Log/log.txt'):
        self.file = logPath
    def write(self , info , infoType = 'INFO'):
        '''append log info in log
        input:
            @info : log text
        '''
        try:
            s = '[%s][%s]%s' % (infoType , time.ctime() , info)
            print s
            f = open(self.file , 'a')
            f.write(s)
            f.write('\n')
            f.close()
        except Exception as e:
            print "Someting wrong when record log. The error info are : %s" %e