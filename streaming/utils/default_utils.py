#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys

def ensureOffset(kvs):
    offsetRanges = []

    def storeOffsetRange(rdd):
        global offsetRanges
        offsetRanges = rdd.offsetRanges()
        return rdd
    def printOffsetRange(rdd):
        for o in offsetRanges:
            print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

    kvs.transform(storeOffsetRange).foreachRDD(printOffsetRange)

def setDefaultEncoding():
    reload(sys)
    sys.setdefaultencoding("utf-8")

def initStreamingContext(appName, masterName, scTime):
    conf = SparkConf().setAppName(appName).setMaster(masterName)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, scTime)
    return ssc
