#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
import itertools
from utils.default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json
import datetime
import time
from pykafka import KafkaClient


# 转换json
def toJson(rdd):
    line = json.loads(rdd[1].encode("UTF-8"))
    line["collect_time"] = int(time.mktime(
        time.strptime(
            datetime.datetime.now().strftime("%Y-%m-%d") + " " + line["collect_time"].encode("utf-8"),
            "%Y-%m-%d %H:%M:%S:%f"
        )))
    return [line]


# newValues新传进来的值
# runningCount
def updateFun(newValues, runningCount):
    newValue = ""
    oldValue = ""
    print "**********************************"
    if newValues:
        newValues.sort(key=lambda k: (k.get("collect_time")), reverse=True)
        # 对新进来的值进行过滤
        newValue = newValues[0]
        print newValues
    print "=================================="
    if runningCount is not None:
        # 对旧值进行过滤
        oldValue = runningCount[0]
        print runningCount
    print "++++++++++++++++++++++++++++++++++"
    return (newValue, oldValue)


# kafka produce
def kafka_produce(message):
    client = KafkaClient(hosts="192.168.108.222:9092")
    topic = client.topics['disconnection_receive_topic']

    with topic.get_producer(delivery_reports=True) as producer:
        producer.produce(message)
        producer.stop()


# 断线时间计算
def disconnection_patrol(lines):

    def update_msg(message, collect_time):
        collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(collect_time))
        change_dict = {'warning_state': 'event', 'warning_name': '断线', 'collect_time': collect_time}
        message.update(change_dict)
        kafka_produce(json.dumps(message))

    if lines[0] and lines[1] != "":
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
        print lines[0].get('collect_time')
        print lines[1].get('collect_time')
        if lines[0].get('collect_time') - lines[1].get('collect_time') > 3:
            print "time out in"
            update_msg(lines[0], lines[0].get('collect_time'))
    else:
        print "time out out"
        if lines[0] == "" and lines[1] != "":
            update_msg(lines[1], lines[1].get('collect_time'))
        elif lines[1] == "" and lines[0] != "":
            update_msg(lines[0], lines[0].get('collect_time'))


# 对每个分区RDD操作
def foreachPartitionFun(rdd):
    def partitionOfRecordsFun(rdd):
        print "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        for item in itertools.chain(rdd):
            print item[0], item[1]
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            disconnection_patrol(item[1])

    rdd.foreachPartition(partitionOfRecordsFun)


if __name__ == "__main__":
    checkpoint_path = "hdfs://localhost:9000/checkpiont/streaming_cp_log"
    kafka_topic_list = ["realdata_receive"]
    broker_list_dit = {"metadata.broker.list": "101.200.194.191:9092"}

    setDefaultEncoding()
    ssc = initStreamingContext("streaming_kafka_deltaT", "local[2]", 4)
    ssc.checkpoint(checkpoint_path)

    kvs = KafkaUtils.createDirectStream(ssc, kafka_topic_list, broker_list_dit)
    deltaT = kvs.flatMap(lambda lines: toJson(lines)).map(lambda x: (x["oid"], x)). \
        updateStateByKey(updateFun).foreachRDD(foreachPartitionFun)

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()
