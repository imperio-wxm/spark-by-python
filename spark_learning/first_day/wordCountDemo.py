#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from operator import add
import os

#WordCount.txt
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

tempdir = '/home'
path = os.path.join(tempdir, "WordCount.txt")

textFile = sc.textFile(path)
#print textFile.collect()
result = textFile.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
output = sorted(result.collect(),key=lambda x:x[1],reverse=True)
#print output

for key,value in output:
    print key.encode('utf-8') + ' frequency:',value
