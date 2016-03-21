#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
import itertools
from utils.default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json
import datetime
import time


def toJson(rdd):
    line = json.loads(rdd[1].encode("UTF-8"))
    # print type(line["oid"].encode("utf-8"))

    change_time = int(time.mktime(
        time.strptime(
            datetime.datetime.now().strftime("%Y-%m-%d") + " " + line["collect_time"].encode("utf-8"),
            "%Y-%m-%d %H:%M:%S:%f"
        )))
    line["collect_time"] = change_time
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
        print newValue
    print "=================================="
    if runningCount is not None:
        # 对旧值进行过滤
        oldValue = runningCount[0]
        print oldValue
    print "++++++++++++++++++++++++++++++++++"
    return (newValue, oldValue)


def disconnection_patrol(lines):
    if lines[0] and lines[1] != "":
        print lines[0].get('collect_time')
        print lines[1].get('collect_time')
        if lines[0].get('collect_time') - lines[1].get('collect_time') > 5:
            print "time out"
    else:
        print "time out"


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
    broker_list_dit = {"metadata.broker.list": "192.168.108.222:9092"}

    setDefaultEncoding()
    ssc = initStreamingContext("streaming_kafka_deltaT", "local[2]", 5)
    ssc.checkpoint(checkpoint_path)

    """
    def deltaT(rdd):
        asset_attr_list = {}
        # line = rdd
        line = json.loads(rdd[1].encode("UTF-8"))

        if not asset_attr_list.has_key(line["oid"]):
            asset_attr_list = line
            # print asset_attr_list
        else:
            change_dict = {
                "asset_id": line["asset_id"],
                "attr_name": line["attr_name"],
                "collect_time": line["collect_time"],
                "attr_id": line["attr_id"],
                "value": line["value"],
                "warring_name": line["warring_name"],
                "warring_state": line["warring_state"]
            }
            asset_attr_list.update(change_dict)
        return line["oid"], asset_attr_list
    """

    kvs = KafkaUtils.createDirectStream(ssc, kafka_topic_list, broker_list_dit)
    events = kvs.flatMap(lambda lines: toJson(lines))
    deltaT = events.map(lambda x: (x["oid"], x))
    deltaT.updateStateByKey(updateFun).foreachRDD(foreachPartitionFun)

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()
