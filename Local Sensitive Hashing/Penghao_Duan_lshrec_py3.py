# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 11:36:35 2018

@author: Administrator
"""

from pyspark import SparkContext
from itertools import combinations
import sys
from collections import defaultdict

class LSH:
    def __init__(self, numOfSig, numOfBands):
        self.numOfSig = numOfSig
        self.numOfBands = numOfBands
        self.inputDict = None
        
    def inputData(self, line):
        movie_list = line.split(",")
        
        return (int(str(movie_list[0])[1:]), movie_list[1:])
    
    def calculationOfJaccard(self, v1, v2):
        set1 = set(v1)
        set2 = set(v2)
        
        return float(len(set1 & set2))/float(len(set1 | set2))
    
    def signature(self, line):
        signatures = []
        
        for num in range(self.numOfSig):
            min_hash = float('Inf')
            
            for movies in line[1]:
                min_hash = min(min_hash, self.HashFunc(int(movies), num))
            
            signatures.append(str(min_hash))
            
        return (line[0], signatures)
    
    def HashFunc(self, row, hashtime):
        return (3 * row + 13 * hashtime) % 100
    
    def bandHash(self):
        sc = SparkContext(appName = 'Inf553')
        input_file = sys.argv[1]
        output_file = sys.argv[2]
#        rdd = sc.textFile('Input.txt')
        rdd = sc.textFile(input_file)
        input_data_rdd = rdd.map(self.inputData)
        input_rdd = input_data_rdd.map(self.signature)
        self.inputDict = dict(input_data_rdd.collect()) 
        counter = 0
        bandVal = self.numOfSig/self.numOfBands
        candidatePairs = sc.emptyRDD()
        
        for band in range(self.numOfBands):
            bandRDD = input_rdd.map(lambda x: (x[0], x[1][counter: counter + bandVal]))
            bandCandidatePairs = bandRDD.map(lambda umDict: (tuple(umDict[1]), umDict[0])).groupByKey().map(lambda x: list(x[1])).flatMap(lambda x: list(combinations(x,2)))
            candidatePairs = candidatePairs.union(bandCandidatePairs)
            counter = counter + bandVal
            
        pairwiseJaccSim = candidatePairs.distinct().map(lambda x: (x[0], x[1], self.calculationOfJaccard(self.inputDict[x[0]], self.inputDict[x[1]]))).flatMap(lambda x: ((x[0], ([(x[1], x[2])])), ((x[1], [(x[0], x[2])])))).reduceByKey(lambda x,y: x+y).sortByKey(ascending = True)
        
        outputRDD = pairwiseJaccSim.map(lambda x: (x[0], dict(x[1]))).map(lambda x: (x[0], sorted(x[1].items(), key = lambda k: (-k[1], k[0])))).map(lambda x: (x[0], x[1][:5])).map(lambda x: ''.join(s for s in ['U' + str(x[0]),':', ','.join('U' + str (movieName) for movieName in sorted ([a[0] for a in x[1]]))])).collect()
        
#        outfile = open('Test.txt', 'w')
        outfile = open(output_file, 'w')        
        for line in outputRDD:
            outfile.write(line)
            outfile.write('\n')
        outfile.close()
        sc.stop()
        
if __name__ == "__main__":
    lsh = LSH(20, 5)
    lsh.bandHash()