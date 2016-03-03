#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    conf = SparkConf().setAppName("spark_streaming_demo").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    # 批次时间一秒
    ssc = StreamingContext(sc, 5)

    # 监听端口
    lines = ssc.socketTextStream('spark-master', 9999)
    #lines = ssc.textFileStream('/home/jabo/software/streaming_test/')

    # 每行按空格分割
    wordCounts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)

    wordCounts.pprint()

    ssc.start()
    ssc.awaitTermination()

