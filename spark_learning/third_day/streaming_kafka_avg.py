#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
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

def initSparkContext(appName, masterName, scTime):
    conf = SparkConf().setAppName(appName).setMaster(masterName)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, scTime)
    return ssc

if __name__ == "__main__":
    setDefaultEncoding()
    ssc = initSparkContext("streaming_kafka_avg", "local[2]", 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})
    kvs.pprint()

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()

