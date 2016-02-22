#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from operator import add
from os import path
import os

# WordCount.txt
"""
Hello World Bye World
Spark Spark Spark
Hadoop Hadoop Hadoop
Tachyon Tachyon Tachyon Tachyon
Kafka Kafka Kafka Kafka Kafka Kafka
This is a demo project.
"""

conf = SparkConf().setAppName("sparkDemo").setMaster("local")
sc = SparkContext(conf=conf)

dirname = path.dirname(path.dirname(__file__))
tempdir = '/home'
#path = os.path.join(tempdir, "WordCount.txt")
path = '/'.join([dirname,'doc','WordCount.txt'])

# 读取过来转换成RDD（RDD是分区的）
textFile = sc.textFile(path)
# print textFile.collect()
result = textFile.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).\
    reduceByKey(add).map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[1],x[0]))
# 多个RDD可并行操作
output = result.collect()
# print output

for key,value in output:
    print key ,value

sc.stop()
