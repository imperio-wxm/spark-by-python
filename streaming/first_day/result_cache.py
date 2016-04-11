#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from operator import add
from os import path

conf = SparkConf().setAppName("sparkDemo").setMaster("local")
sc = SparkContext(conf=conf)

# 读取过来转换成RDD（RDD是分区的）
textFile = sc.textFile('hdfs://localhost:9000/test/WordCount.txt')
# print textFile.collect()
result = textFile.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).\
    reduceByKey(add).map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
# 多个RDD可并行操作
output = result.collect()
# print output

def output_result(output):
    #for key,value in output:
        #print key ,value
    print output

# 调用foreach
result.foreach(output_result)

# 延迟加载操作，执行action时才触发
# cache实际上调用的时persist方法，时persistent的特殊缓存方式，将RDD放入内存当中
# 可以设置缓存级别
"""
INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 7.5 KB, free 530.1 MB)
INFO storage.MemoryStore: ensureFreeSpace(4638) called with curMem=204056, maxMem=556038881
INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 4.5 KB, free 530.1 MB)
INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:46823 (size: 4.5 KB, free: 530.3 MB)
"""
result.cache()
result.collect()

# 保存到HDFS
#result.saveAsTextFile('hdfs://localhost:9000/test/output_1')

sc.stop()


