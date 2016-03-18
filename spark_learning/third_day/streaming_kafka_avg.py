#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
# utils.default_utils的import必须将utils文件夹复制到spark目录下的python文件夹内才能引用成功
from utils.default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json

count = 0

def updateFun(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def sumFun(lines):
    line = json.loads(lines[1].encode("UTF-8"))
    #print type(line["oid"].encode("utf-8"))
    # return {"oid":line["oid"].encode("utf-8"),"value":line["value"]}
    return ("value", line["value"])

def reduceFun(rdds):
    global count
    for rdd in rdds.offsetRanges():
        count += 1
        print "count================" + str(count)

if __name__ == "__main__":
    setDefaultEncoding()
    ssc = initStreamingContext("streaming_kafka_avg", "local[2]", 5)
    ssc.checkpoint("hdfs://localhost:9000/checkpiont/streaming_cp_log")

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})
    kvs.map(lambda value: sumFun(value)).reduceByKey(reduceFun).\
        updateStateByKey(updateFun).pprint()

    kvs.foreachRDD(reduceFun)

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()


