#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setMaster("local[2]").setAppName("streaming_kafka_json")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 3)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})

    lines = kvs.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)
    lines.pprint()

    # 手动同步kafka offset
    offsetRanges = []

    def storeOffsetRanges(rdd):
         global offsetRanges
         offsetRanges = rdd.offsetRanges()
         return rdd

    def printOffsetRanges(rdd):
         for o in offsetRanges:
             print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

    kvs.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)

    ssc.start()
    ssc.awaitTermination()