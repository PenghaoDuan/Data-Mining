# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 20:06:40 2018

@author: Administrator
"""

import sys
import random
import math
import itertools
import datetime

negativeBorder = set()

def openData(data):
    transform_data =[]
    input_data = data.readlines()
    
    for data_line in input_data:
        line_split = data_line.strip().split(',')
        line_items = []
        for x in line_split:
            processed_item = x.replace('(','').replace(')','').replace(' ','')
            line_items.append(processed_item)
        transform_data.append(line_items)

    return transform_data

def sampling(data, i, p):
    sample = []
    random.seed(i)
    for x in range(1,31):
        index = random.randint(1,len(data))
        sample.append(rawData[index])
    
    return sample
    
def generateFrequentItemsets(basket, size, previousFrequentItemsets, new_support):
    frequentItemsetsCandidates = {}
    result = []
    
    for lines in basket:
        for subset in itertools.combinations(lines, size):
            frequentItemsetsCandidates.setdefault(subset, 0)
            frequentItemsetsCandidates[subset] += 1
            
    for k,v in frequentItemsetsCandidates.items():
        if int(v) >= new_support:
            result.append(list(k))
        else:
            if len(k) == 1:
                negativeBorder.add(k)
            else:
                count = 0
                
                for subset in itertools.combinations(k, size -1):
                    if list(subset) not in previousFrequentItemsets:
                        count += 1
                        
                if count == 0:
                    negativeBorder.add(k)
    
    return (sorted(result))
    
    
if __name__=='__main__':
    starttime = datetime.datetime.now()
    inputdata = open('dataset.txt','r')
    rawData = openData(inputdata)
    
    support_ratio = float(4/15)
    proportion = 0.1
    support = len(rawData) * support_ratio
    new_support = len(rawData) * proportion * support_ratio
    
    size = 1
    frequentItemsets = []
    allFrequentItemsets = []
    
    start = True
    numOfIterations = 1 
    basket_after_sampling = sampling(rawData, numOfIterations,proportion)   
    
    while start:
        start = False
        for x in range(99):
            frequentItemsets = generateFrequentItemsets(basket_after_sampling, size, frequentItemsets, new_support)
            
            if len(frequentItemsets) > 0:
                allFrequentItemsets.append(frequentItemsets)
            
            size += 1
            
            if frequentItemsets == []:
                d = {}
                for basket in rawData:
                    for ele in negativeBorder:
                        if set(ele) <= set(basket):
                            d.setdefault(ele, 0)
                            d[ele] += 1
                    
                another_time = 0
                
                for k, v in d.items():
                    
                    if v >= support:
                        output =[]
                        output.append("Sample Created:")
                        output.append(basket_after_sampling)
                        output.append("frequent itemsets")
                        output.append(allFrequentItemsets)
                        output.append("negative border")
                        output.append(list(negativeBorder))
                        
                        frequencyInAll = {}
                        for basket in rawData:
                            for ele1 in allFrequentItemsets:
                                for ele2 in ele1:
                                    if set(ele2) <= set(basket):
                                        frequencyInAll.setdefault(tuple(ele2), 0)
                                        frequencyInAll[tuple(ele2)] += 1
                                        
                        false_negatives = sorted([x for x in frequencyInAll if frequencyInAll[x] < support])
                            
                        print("False Negatives:"+false_negatives)
                        numOfIterations += 1
                        another_time += 1
                        
                        size = 1
                        frequentItemsets = []
                        allFrequentItemsets = []
                        negativeBorder =set()
                        basket_after_sampling = sampling(rawData, numOfIterations)
                        break
                
                d1={}
                for basket in rawData:
                    for ele1 in allFrequentItemsets:
                        for ele2 in ele1:
                            if set(ele2) <= set(basket):
                                d1.setdefault(tuple(ele2), 0)
                                d1[tuple(ele2)] += 1
                                
                d2 = sorted([ x for x in d1 if d1[x] >= support ])
                
                if another_time == 0:
                    output =[]
                    output.append("Sample Created:")
                    output.append(basket_after_sampling)
                    output.append("frequent itemsets")
                    output.append(list(d2))
                    output.append("negative border")
                    output.append(list(negativeBorder))
                    
                    endtime = datetime.datetime.now 
                    print('-------' + numOfIterations + '--------------')
                    print((endtime - starttime).seconds)
                    
                    sys.exit()
                    
        start = True
        break
#                for x in d1:
#                    if d1[x] >= support:
#                        
                
                
                
                        
                    
    
    
    
#    result = toivonen(data, support, support_ratio)
#    
#    list_false_negatives, iteration_counts = result[0], result[1]
#    
#    for false_negative in list_false_negatives:
#        print('False Negatives:', false_negative)
    
    
#    endtime = datetime.datetime.now()
#    
    
        
        
#        print(random.randint(1,100))
#    transform = []
#    transform_1 = []
#    a = inputdata_1.readlines()
#    b = inputdata_2.readlines()
    
#    for data_line in a:
#        transform.append(data_line.strip().split(','))
#    
#    line_item =[]
#    for data_line in b:
#        now = data_line.strip().split(',')
#        for x in now:
#            z = x.replace('(','').replace(')','').replace(' ','')
#            line_item.append(z)
#        transform_1.append(line_item)
#    print(transform[0][1])
#    c = len(transform_1[0][1])
#    print(transform_1[0][0])
#    support_ratio = 4/15;
#    raw_data = openData(inputdata)
 