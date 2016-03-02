#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from operator import add
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# standalone模式
conf = SparkConf().setAppName("sparkDemo").setMaster('spark://spark-master:7077')
sc = SparkContext(conf=conf)

# 读取过来转换成RDD（RDD是分区的）
textFile = sc.textFile('hdfs://localhost:9000/test_1/WordCount.txt')
# print textFile.collect()
result = textFile.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).\
    reduceByKey(add).map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
# 多个RDD可并行操作
output = result.collect()
# print output

def output_result(output):
    for key,value in output:
        print key.decode('utf-8') ,value

# 调用foreach
#result.foreach(output_result)
output_result(output)

# 保存到HDFS
#result.saveAsTextFile('hdfs://localhost:9000/test/output_1')

sc.stop()


