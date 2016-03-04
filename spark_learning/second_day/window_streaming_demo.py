#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[2]").setAppName("window_streaming_demo")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 3)
    # 设置checkpoint到hdfs上
    ssc.checkpoint("hdfs://localhost:9000/checkpiont/streaming_cp_log")

    lines = ssc.socketTextStream("spark-master", 9999)
    wordCounts = lines.flatMap(lambda line: line.split(' '))\
        .map(lambda word: (word,1))\
        .reduceByKey(lambda x, y: (x+y))

    # 每隔3s，统计一次前6s的数据
    windows = wordCounts.reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, 6, 3)
    windows.pprint()

    ssc.start()
    ssc.awaitTermination()