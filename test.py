# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 12:02:55 2018

@author: Administrator
"""

from pyspark import SparkContext

#def printf(iterator):
#    par = list(iterator)
#    print('partition:', par)
#
#sc = SparkContext(appName = "inf 551")
#print(sc.parallelize([1,2,3,4,5],2).collect())
#sc.parallelize([1,2,3,4,5],2).foreachPartition(printf)

#sc.stop()
#
#def add(a,b):
#    return a+b
#def sumf(iterator):
#    sum=0
#    count=0
#    for v in iterator:
#        sum+=v
#        count+=1
#    yield(sum,count)
#    
#sc=SparkContext(appName = 'new')
#print(sc.parallelize([1,2,3],2).reduce(add))
#data = [5,4,4,1,2,3,3,1,2,5,4,5]
#pdata = sc.parallelize(data,2)
#print(pdata.reduce(lambda x, y: max(x,y)))
#
#print(pdata.mapPartitions(sumf))
#
#print(pdata.reduce(add))
#print(pdata.reduce(max))

matrixa = open("file-A.txt").read()
matrixb = open("file-B.txt").read()

#    matrixA = matrixa.replace('(','').replace(')','').replace('[','').replace(']','')
#    matrixB = matrixb.replace('(','').replace(')','').replace('[','').replace(']','')
#    
sc = SparkContext(appName = "INF553")
    
matrixA_rdd = sc.textFile(matrixa)
matrixB_rdd = sc.textFile(matrixb)
    
print(matrixA_rdd.map(lambda x: (x.split(',')), (x.replace('(,''), (x.replace(')',''),(x.replace('[',''),x.replace(']','')).map(lambda y: (((y[0],1),("A", y[1], y[2:])))).collect())