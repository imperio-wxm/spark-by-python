#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
# utils.default_utils的import必须将utils文件夹复制到spark目录下的python文件夹内才能引用成功
from utils.default_utils import setDefaultEncoding, initSparkContext, ensureOffset

if __name__ == "__main__":
    setDefaultEncoding()
    ssc = initSparkContext("streaming_kafka_avg", "local[2]", 5)

    kvs = KafkaUtils.createDirectStream(ssc, ["realdata_receive"], {"metadata.broker.list": "192.168.108.222:9092"})
    kvs.pprint()

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()


