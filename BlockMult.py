 mo2# -*- coding: utf-8 -*-
"""
Created on Thu Sep 20 13:43:16 2018

@author: Penghao DUAN
"""
from pyspark import SparkConf, SparkContext
import numpy as np
import sys

def smallBlock_mul(a,b):
#    Transforming into Matrix form (A)
    if len(a) == 12:
        A =[[int(a[2]), int(a[5])], [int(a[8]),int(a[11])]]
    
    if len(a) == 9:
        if a[0] == '1' and a[1] == '1':
            if a[3] == '1' and a[4] == '2':
                if a[6] =='2' and a[7] =='1':
                    A = [[int(a[2]), int(a[5])], [int(a[8]),0]]
                else:
                    A = [[int(a[2]), int(a[5])], [0,int(a[8])]]
        elif a[0] == '1' and a[1] =='1':
            A = [[int(a[2]), 0], [int(a[5]),int(a[8])]]
        else:
            A = [0,[int(a[2])], [int(a[5]),int(a[8])]]
    
    if len(a) == 6:
        if a[0] == '1' and a[1] =='1':
            if a[3] == '1' and a[4] == '2':
                A = [[int(a[2]), int(a[5])], [0,0]] 
            elif a[3] =='2' and a[4] =='1':
                A = [[int(a[2]), 0], [int(a[5]),0]]
            else:
                A = [[int(a[2]), 0], [0,int(a[5])]]
        elif a[0] == '1' and a[2] =='2':
            if a[3] == '2' and a[4] =='1':
                 A = [[0, int(a[2])], [int(a[5]),0]]
            else:
                 A = [[0, int(a[2])], [0,int(a[5])]]
        else:
            A = [[0, 0], [int(a[2]),int(a[5])]]
            
    if len(a) == 3:
        if a[0] == '1' and a[1] == '1':
            A = [[int(a[2]), 0], [0,0]]
        elif a[0] == '1' and a[1] =='2':
            A = [[0, int(a[2])], [0,0]]
        elif a[0] == '2' and a[1] == '1':
            A = [[0, 0], [int(a[2]),0]]
        else:
             A = [[0, 0], [0,int(a[2])]]
    
    if len(a) == 0:
        A=[[0,0], [0,0]]
     
#   Transforming into Matrix form (B)
    if len(b) == 12:
        B =[[int(b[2]), int(b[5])], [int(b[8]),int(b[11])]]
    
    if len(b) == 9:
        if b[0] == '1' and b[1] == '1':
            if b[3] == '1' and b[4] == '2':
                if b[6] =='2' and b[7] =='1':
                    B = [[int(b[2]), int(b[5])], [int(b[8]),0]]
                else:
                    B = [[int(b[2]), int(b[5])], [0,int(b[8])]]
        elif b[0] == '1' and b[1] =='1':
            B = [[int(b[2]), 0], [int(b[5]),int(b[8])]]
        else:
            B = [0,[int(b[2])], [int(b[5]),int(b[8])]]
    
    if len(b) == 6:
        if b[0] == '1' and b[1] =='1':
            if b[3] == '1' and b[4] == '2':
                B = [[int(b[2]), int(b[5])], [0,0]] 
            elif b[3] =='2' and b[4] =='1':
                B = [[int(b[2]), 0], [int(b[5]),0]]
            else:
                B = [[int(b[2]), 0], [0,int(b[5])]]
        elif b[0] == '1' and b[2] =='2':
            if b[3] == '2' and b[4] =='1':
                 B = [[0, int(b[2])], [int(b[5]),0]]
            else:
                 B = [[0, int(b[2])], [0,int(b[5])]]
        else:
            B = [[0, 0], [int(b[2]),int(b[5])]]
            
    if len(b) == 3:
        if b[0] == '1' and b[1] == '1':
            B = [[int(b[2]), 0], [0,0]]
        elif b[0] == '1' and b[1] =='2':
            B = [[0, int(b[2])], [0,0]]
        elif b[0] == '2' and b[1] == '1':
            B = [[0, 0], [int(b[2]),0]]
        else:
             B = [[0, 0], [0,int(b[2])]]
    
    if len(b) == 0:
        B=[[0,0], [0,0]]
    
    return np.dot(A,B)
            
         
def reducer(x):
    matrixA = []
    matrixB = []
    result =[]
    
    for val in x:
        if "A" == val[0]:
            matrixA.append(val)
        else:
            matrixB.append(val) # Now (''A', k, value) in matrixA
    
    for x in matrixA:
        for y in matrixB:
            if x[1] == y[1]:
                result.append(smallBlcok_mul(x[2],y[2]))
                
    return sum (result)
            
    
    return result
if __name__=="__main__":
    matrixa, matrixb,output_file_path= sys.argv[1:]
    sc.stop()
#    matrixa = open("file-A.txt").read()
#    matrixb = open("file-B.txt").read()
    
    sc = SparkContext(appName = "INF553")

    
    matrixA = sc.textFile(matrixa)
    matrixB = sc.textFile(matrixb)
    
    for p in range(4):
        A = matrixA.map(lambda x: ((x.split(',')), (x.replace('(','')), 
                                    (x.replace(')','')), (x.replace('[','')),
    (x.replace(']',''))).map(lambda y: (((y[0],1),("A", y[1], y[2:])))).collect()
        
        B = matrixB.map(lambda x: ((x.split(',')), (x.replace('(','')), 
                                  (x.replace(')','')), (x.replace('[','')),
    (x.replace(']',''))).map(lambda y: (((p,y[1]),("B", y[0], y[2:])))).collect()
    
    input_data = sc.parallelize(A+B)
    
    output = input_data.groupByKey().flatMap(
            lambda x: reducer(x[1])).map(lambda a: ",".join([x[0] + a).collect()
    output_file = open(output_file_path, 'w')
    
    for item in output:
        output_file.write("%s\n"%item)