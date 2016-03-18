#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark.streaming.kafka import KafkaUtils
import itertools
from utils.default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json
import datetime
import time

deltaT_dict = {}

if __name__ == "__main__":
    checkpoint_path = "hdfs://localhost:9000/checkpiont/streaming_cp_log"
    kafka_topic_list = ["realdata_receive"]
    broker_list_dit = {"metadata.broker.list": "192.168.108.222:9092"}

    setDefaultEncoding()
    ssc = initStreamingContext("streaming_kafka_deltaT", "local[2]", 5)
    ssc.checkpoint(checkpoint_path)


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


    def toJson(rdd):
        line = json.loads(rdd[1].encode("UTF-8"))
        # print type(line["oid"].encode("utf-8"))

        chenge_time = int(time.mktime(
            time.strptime(
                datetime.datetime.now().strftime("%Y-%m-%d") + " " + line["collect_time"].encode("utf-8"),
                "%Y-%m-%d %H:%M:%S:%f"
            )))
        line["collect_time"] = chenge_time
        return [line]


    # newValues新传进来的值
    # runningCount
    def updateFun(newValues, runningCount):
        oldValues = ""
        print "**********************************"

        """
        print newValues.sort(key=lambda k:
            k[int(time.mktime(
                time.strptime(
                    datetime.datetime.now().strftime("%Y-%m-%d") + " " + newValues[0]["collect_time"].encode("utf-8"),
                    "%Y-%m-%d %H:%M:%S:%f"
                )))])
        """

        newValues.sort(key=lambda k: (k.get("collect_time")),reverse=True)
        print newValues[0]
        print "=================================="
        if runningCount is not None:

            oldValues = runningCount[0]
            print runningCount[0]
        print "++++++++++++++++++++++++++++++++++"

        return (newValues[0], oldValues)


    kvs = KafkaUtils.createDirectStream(ssc, kafka_topic_list, broker_list_dit)
    events = kvs.flatMap(lambda lines: toJson(lines))
    # deltaT = events.map(lambda x: (x["oid"], datetime.datetime.now().strftime("%Y-%m-%d") + " " + x["collect_time"].encode("utf-8")))
    deltaT = events.map(lambda x: (x["oid"], x))


    # deltaT.pprint()
    # deltaT.foreachRDD(foreachPartitionFun)

    def foreachPartitionFun(rdd):
        def partitionOfRecordsFun(rdd):
            print "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            global deltaT_dict
            for item in itertools.chain(rdd):
                print item[0], item[1]
                """
                change_dict = {
                        item[0]:item[1]
                }
                if deltaT_dict.has_key(item[0]):
                    print "you"
                    deltaT_dict.update(change_dict)
                else:
                    print "mei"
                    deltaT_dict[item[0]] = item[1]
                    print deltaT_dict
                """
                # print deltaT_dict

        rdd.foreachPartition(partitionOfRecordsFun)


    deltaT.updateStateByKey(updateFun).foreachRDD(foreachPartitionFun)

    ensureOffset(kvs=kvs)

    ssc.start()
    ssc.awaitTermination()
