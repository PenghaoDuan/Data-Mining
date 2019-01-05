# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 11:28:35 2018

@author: Administrator
"""

import sys
import math
import heapq
import copy



def calEuclideanDist(a, b):
    x1 = a[0][0]
    x2 = b[0][0]
    y1 = a[0][1]
    y2 = b[0][1]
    z1 = a[0][2]
    z2 = b[0][2]
    w1 = a[0][3]
    w2 = b[0][3]
    diffx = x2-x1
    diffy = y2-y1
    diffz = z2-z1
    diffw = w2-w1
    Sum = (diffx**2)+(diffy**2)+(diffz**2)+(diffw**2)
    distance = math.sqrt(Sum)
    return distance

def calCentroid(a, b):
    x1 = a[0][0]
    x2 = b[0][0]
    y1 = a[0][1]
    y2 = b[0][1]
    z1 = a[0][2]
    z2 = b[0][2]
    w1 = a[0][3]
    w2 = b[0][3]
    cluster1 = a[1]
    l1=len(cluster1)
    cluster2 = b[1]
    l2 = len(cluster2)
    cluster1.extend(cluster2)
    length = len(cluster1)
    avgx = ((l1*x1)+(l2*x2))/length
    avgy = ((l1*y1)+(l2*y2))/length
    avgz = ((l1*z1)+(l2*z2))/length
    avgw = ((l1*w1)+(l2*w2))/length
    return [[avgx,avgy,avgz,avgw,""],cluster1]
    
#f = open(sys.argv[1])
f = open('iris.dat')
#k = int(sys.argv[2])
k = 3
data = f.readlines()
input_data = []
for line in data:
    if line.strip()!= '':
        line = line.split(',')
        line = [i.strip() for i in line]
        line[0] = float(line[0])
        line[1] = float(line[1])
        line[2] = float(line[2])
        line[3] = float(line[3])
        input_data.append(line)

dict_input = {}
for i in range(len(input_data)):
    dict_input[i]=[input_data[i],[i]]
n = len(dict_input)    

ori_dict_input = copy.deepcopy(dict_input)

while(len(dict_input) > k):
    heap = [] 
    distancedict = {}
    listt = []
    for a in range(len(ori_dict_input)-1):
        for b in range(a+1,len(ori_dict_input)):
            if a in dict_input and b in dict_input:
                vec1 = dict_input[a]
                vec2 = dict_input[b]
                ed = calEuclideanDist(vec1, vec2)
                if ed not in distancedict.keys():
                    distancedict[ed] = (a,b)
                    heapq.heappush(heap, ed)

    smallestdistance = heapq.heappop(heap)
    coord = distancedict[smallestdistance]
    value1 = dict_input[coord[0]]
    value2 = dict_input[coord[1]]
    newcoord = calCentroid(value1,value2)
    del(dict_input[coord[0]])
    del(dict_input[coord[1]])
    dict_input[n]= newcoord
    ori_dict_input[n] = newcoord
    n = n + 1
    
wrong = 0
o = 20
output = []
for each in dict_input:
    cluster = dict_input[each][1]
    output.append(cluster)
    
    names = {}
    answer = []
    
    for x in cluster:
        names[ori_dict_input[i][0][4]] = names.get(ori_dict_input[i][0][4],0)+1
        clustername = max(names,key = names.get)
        answer.append(ori_dict_input[i][0])
    
    for y in names:
        if y != clustername:
            wrong += names[y]

print 'Cluster 1:' + str(sorted(output[1]))
print 'Cluster 2:' + str(sorted(output[2]))
print 'Cluster 3:' + str(sorted(output[0]))

print 'Precision =' + str(float(x)/len(input_data))
print 'Recall =' + str((float)(len(input_data) - o)/len(input_data))

