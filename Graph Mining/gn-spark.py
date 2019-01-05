# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 14:55:36 2018

@author: Administrator
"""

from pyspark import SparkContext
import sys
import Queue
import math

inputfile = sys.argv[1]#"input.json
betweenness_output = sys.argv[2]#"PenghaoDUAN_betweenness.txt"

def Dict1(m,n):
	Dict2 = {}
	for i in m:
		llist = [0] * n
		for j in m[i]:
			llist[j-1] = j
		Dict2[i] = llist
	return Dict2


def cutfind(allMap,firstnode,secondnode):
	que = Queue.Queue()
	que.put((firstnode,0))
	k = {}
	k[firstnode] = (0,1)
	while not que.empty():
		node,depth = que.get()
		for item in list(allMap[node]):
			if item > 0:
				if item == secondnode:
					return False
				if k.get(item,0) == 0:
					que.put((item,depth + 1))
					k[item] = (depth + 1, 1)
	return True

def checkcommunity(allMap,root):
	k = {}
	result = set()
	que = Queue.Queue()
	que.put((root,0))
	k[root] = (0,1)
	result.add(root)
	while not que.empty():
		node,depth = que.get()
		for i in list(allMap[node]):
			if i > 0:
				if k.get(i,0) == 0:
					que.put((i,depth + 1))
					k[i] = (depth + 1, 1)
					result.add(i)
	return (result)


def allBFS(allMap,root):
	k = {}
	result = []
	que = Queue.Queue()
	que.put((root,0,[[root]]))
	k[root] = (0,1,[[root]])
	while not que.empty():
		node,depth,paths = que.get()
		for item in list(allMap[node]):
			if k.get(item,0) == 0:
				newPaths = []
				for p in paths:
					newPaths.append(p+[item])
				que.put((item,depth + 1,newPaths))
				k[item] = (depth + 1, 1,newPaths)
				result.append(item)
			else:
				oldDepth,repeat,oldPath = k[item]
				if oldDepth > depth:
					newPaths = []
					for p in paths:
						newPaths.append(p+[item])	
					oldPath.extend(newPaths)
					k[item] = (oldDepth,repeat + 1,oldPath)
	yield (k,result)


def getweight(k,target):
	weight = {}
	point = {}
	for i in range(1,len(target)+1):
		item = target[-i]
		_,rep,paths = k[item]
		parent = set()
		for p in paths:
			parent.add(p[-2])
		newPoint = float(1 + point.get(item,0)) / float(rep)
		for n in parent:
			if n < item:
				weight[(n,item)] = newPoint
			else:
				weight[(item,n)] = newPoint
			if point.get(n,-1) == -1:
				point[n] = newPoint
			else:
				point[n] += newPoint
	yield weight.items()


def edgecut(mapp,firstnode,secondnode):
	a = mapp.get(firstnode)
	a[secondnode-1] = 0
	mapp[firstnode] = a
	b = mapp.get(secondnode)
	b[firstnode-1] = 0
	mapp[secondnode] = b
	return mapp

def nu(mapp):
	k = {}
	for i in mapp:
		value = mapp[i]
		k[i] = len(value)
	return k

def Modular(origMap,order,communities,coun):
	k = nu(origMap)
	allMap = Dict1(origMap,671)
	oriMap = Dict1(origMap,671)
	result = []
	Q = 0
	for i in communities:
		i = list(i)
		i = sorted(i)
		for m in range(len(i)-1):
			a = i[m]
			mapp = oriMap.get(a)
			for n in range(m+1,len(i)):
				b = i[n]
				Aij = 0
				if mapp[b-1] == b:
					Aij = 1
				Q += (Aij - (float(k[a] * k[b]) / (2 * coun))) / (2 * coun)
	result.append(((Q,len(communities)),communities))
	for i in order:
		firstnode,secondnode = i
		edgecut(allMap,firstnode,secondnode)
		if cutfind(allMap,firstnode,secondnode):
			num = []
			for ii in communities:
				if firstnode not in ii:
					num.append(ii)
			num.append(checkcommunity(allMap,firstnode))
			num.append(checkcommunity(allMap,secondnode))
			communities = num
		else:
			continue
		Q = 0
		for i in communities:
			i = sorted(list(i))
			for m in range(len(i)-1):
				a = i[m]
				mapp = oriMap.get(a)
				for n in range(m+1,len(i)):
					b = i[n]
					Aij = 0
					if mapp[b-1] == b:
						Aij = 1
					Q += (Aij - (float(k[a] * k[b]) / (2 * coun))) / (2 * coun)
		result.append(((Q,len(communities)),communities))
	return result


sc = SparkContext(appName="PenghaoDuan")
sc.setLogLevel("ERROR")
ratings = sc.textFile(inputfile)
rheader = ratings.first()
rating = ratings.filter(lambda x: x!= rheader)
rat = rating.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).groupByKey().sortByKey(True)
pall = rat.cartesian(rat).filter(lambda x: x[0][0] != x[1][0])
common = pall.map(lambda x: ((x[0][0],x[1][0]),set(x[0][1]).intersection(set(x[1][1])))).filter(lambda x: len(x[1]) >=3 )
cp = common.map(lambda x: (x[0][0],x[0][1])).groupByKey()

mapp = cp.collectAsMap()
edges = cp.flatMap(lambda x: allBFS(mapp,x[0]))
edge = edges.flatMap(lambda x: getweight(x[0],x[1])).flatMap(lambda x: x).groupByKey().map(lambda x: (x[0],(float(sum(x[1])) / float(2))))

result = sorted(edge.collect())

betweennessfile = open(betweenness_output, 'w')
for r in result:
	betweennessfile.write("(" + "%s,%s,%s" % (r[0][0],r[0][1],math.floor(r[1]*10)/10) + ")"+ "\n") 
betweennessfile.close()
