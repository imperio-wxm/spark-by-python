#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")

    conf = SparkConf().setMaster("local[2]").setAppName("streaming_kafka_json")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("hdfs://localhost:9000/checkpiont/streaming_cp_log")

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})

    # 设置累计计算函数
    def updateFunction(newValues, runningCount):
        """
        :param newValues: 新值
        :param runningCount: 传进的值
        :return: 计数累加
        """
        count = 0
        if runningCount is None:
            runningCount = 0
        else:
            count += 1
        return sum(newValues, runningCount)

    # 1. lines是一个元组，lines[1]取第二个元素
    # 2. 默认为unicode编码，转换成utf-8
    # 3. json.loads()转换成json
    def toJson(lines):
        line = json.loads(lines[1].encode("UTF-8"))
        #print type(line["oid"].encode("utf-8"))
        return [line["oid"].encode("utf-8")]

    # 对oid进行统计个数
    lines = kvs.flatMap(lambda x: toJson(x)).map(lambda word:(word, 1)).reduceByKey(lambda x,y: x+y)
    stateDstream = lines.updateStateByKey(updateFunction)
    stateDstream.pprint()

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