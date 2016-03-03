#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    # 设置累计计算函数
    def updateFunction(newValues, runningCount):
        """
        :param newValues: 新值
        :param runningCount: 传进的值
        :return: 计数累加
        """
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    conf = SparkConf().setAppName("update_state").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 3)
    # 设置checkpoint到hdfs上
    ssc.checkpoint("hdfs://localhost:9000/checkpiont/streaming_cp_log")

    lines = ssc.socketTextStream("spark-master", 9999)

    wordCounts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))\
        .reduceByKey(lambda x,y: x+y)

    # 传入函数，每次计算的都是重复值的累加
    stateDstream = wordCounts.updateStateByKey(updateFunction)
    stateDstream.pprint()

    ssc.start()
    ssc.awaitTermination()

