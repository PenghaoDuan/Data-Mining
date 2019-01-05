# -*- coding: utf-8 -*-
"""
Created on Sat Sep 22 12:58:06 2018

@author: Administrator
"""
import numpy as np
from pyspark import SparkConf, SparkContext
 a = 'abc'
print(a[1,2])
#
#sc.stop()
#matrixa = open("file-A.txt").read()
#matrixb = open("file-B.txt").read()
#
#sc = SparkContext(appName = "INF553")
#
#matrixA = sc.textFile(matrixa)
#matrixB = sc.textFile(matrixb)
#
#matrixA.map(lambda x: (
#        (x.split(',')), 
#        (x.replace('(','')),
#        (x.replace('(',''))))
#
#for i in range(1,4):
#    print(i)

#, (x.replace('(',''), (x.replace(')',''),(
#        x.replace('[',''),x.replace(']','')).map(lambda y: (((y[0],1),("A", y[1], y[2:])))).collect())
#sc.stop()
# 
#a = [[1,0], [0,1]]
#b = [[4,1], [2,2]]
#d = np.dot(a,b) + np.dot(a,b)
#print(d)
#
#c = []
#c.append(d)
#
#sc = SparkContext(appName = "try")
#data = sc.parallelize([1,2,3,4,5,1,3,5],2)
#p=data.map(lambda x: x if x%2 == 0 else None).collect()
#
#sc.stop()