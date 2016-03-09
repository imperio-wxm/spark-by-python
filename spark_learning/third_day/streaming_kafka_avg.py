#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
# utils.default_utils的import必须将utils文件夹复制到spark目录下的python文件夹内才能引用成功
from utils.default_utils import setDefaultEncoding, initSparkContext, ensureOffset
import json

def updateFun(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def avgFun(lines):
    line = json.loads(lines[1].encode("UTF-8"))
    #print type(line["oid"].encode("utf-8"))
    return [{"oid":line["oid"].encode("utf-8"),"value":line["value"]}]


if __name__ == "__main__":
    setDefaultEncoding()
    ssc = initSparkContext("streaming_kafka_avg", "local[2]", 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})
    kvs.map(lambda lines: avgFun(lines)).pprint()

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()


