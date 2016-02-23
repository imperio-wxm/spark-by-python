#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from os import path
import re

conf = SparkConf().setAppName('mostWords').setMaster('local')
sc = SparkContext(conf=conf)

dirname = path.dirname(path.dirname(__file__))
filename = 'MostWords.txt'
#path = os.path.join(tempdir, "WordCount.txt")
path = '/'.join([dirname,'doc',filename])

textFile = sc.textFile(path)

# 将每一行按分割长度进行map
# reduce所有行里，len最长的map映射
words_count =textFile.map(lambda line: len(re.split('(\\s*,\\s*|\\s+)',line))).reduce(lambda a, b: a if(a > b) else b)

#words_count = textFile.map(lambda line: len(line)).collect()

print words_count