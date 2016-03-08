#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
from pyspark.sql import SQLContext

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")

    conf = SparkConf().setMaster("local[2]").setAppName("streaming_kafka_json")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    sqlC = SQLContext(sc)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})


    # 1. lines是一个元组，lines[1]取第二个元素
    # 2. 默认为unicode编码，转换成utf-8
    # 3. json.loads()转换成json
    def toJson(lines):
        line = json.loads(lines[1].encode("utf-8"))
        print line["oid"]

    lines = kvs.map(lambda x: toJson(x))
    lines.pprint()

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