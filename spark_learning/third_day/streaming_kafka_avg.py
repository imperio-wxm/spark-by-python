#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")
    conf = SparkConf().setAppName("streaming_kafka_avg").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})


    offsetRanges = []

    def storeOffsetRange(rdd):
        global offsetRanges
        offsetRanges = rdd.offsetRanges()
        return rdd

    def printOffsetRange(rdd):
        for o in offsetRanges:
            print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

    kvs.transform(storeOffsetRange).foreachRDD(printOffsetRange)

    ssc.start()
    ssc.awaitTermination()

