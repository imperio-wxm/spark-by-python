#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    conf = SparkConf().setAppName("streaming_kafka_1").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})

    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    # 每行按空格分割
    """
    wordCounts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    wordCounts.pprint()
    """
    ssc.start()
    ssc.awaitTermination()