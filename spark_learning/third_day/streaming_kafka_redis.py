#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import itertools
#from default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json
import datetime
import time
import sys
import redis

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

"""
# kafka produce
def kafka_produce(message):
    client = KafkaClient(hosts="192.168.108.222:9092")
    topic = client.topics['disconnection_receive_topic']

    with topic.get_sync_producer() as producer:
        producer.produce(message)
        producer.stop()

class ConnectionPool:
    @staticmethod
    def kafka_produce():
        client = KafkaClient(hosts="192.168.108.222:9092")
        topic = client.topics['disconnection_receive_topic']
        return topic

        #with topic.get_sync_producer() as producer:
            #producer.produce(message)
            #producer.stop()
            #
"""
            
class RedisDBConfig:  
    HOST = 'spark-master'  
    PORT = 6379  
    DBID = 0 

def operator_status(func):  
    def gen_status(*args, **kwargs):  
        error, result = None, None  
        try:  
            result = func(*args, **kwargs)  
        except Exception as e:  
            error = str(e)  
  
        return {'result': result, 'error':  error}  
  
    return gen_status

class RedisCache(object):  
    def __init__(self):  
        if not hasattr(RedisCache, 'pool'):  
            RedisCache.create_pool()  
        self._connection = redis.Redis(connection_pool = RedisCache.pool) 

    @staticmethod  
    def create_pool():  
        RedisCache.pool = redis.ConnectionPool(  
                host = RedisDBConfig.HOST,  
                port = RedisDBConfig.PORT,  
                db   = RedisDBConfig.DBID)

    @operator_status  
    def set_data(self, key, value):  
        return self._connection.set(key, value)

    @operator_status
    def lpush_data(self, value):
        return self._connection.lpush("realtime_data_queue",value)

"""
# 断线时间计算
def disconnection_patrol(lines):
    #def kafka_produce(message):
        #client = KafkaClient(hosts="192.168.108.222:9092")
        #topic = client.topics['disconnection_receive_topic']

        #with topic.get_producer(delivery_reports=True) as producer:
            #producer.produce(message)

    def update_msg(message, collect_time):
        collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(collect_time))
        change_dict = {'warning_state': 'event', 'warning_name': '断线', 'collect_time': collect_time}
        message.update(change_dict)
        topic = ConnectionPool.kafka_produce() 
        with topic.get_sync_producer() as producer:
            producer.produce(json.dumps(message))
        producer.stop()
        #kafka_produce(json.dumps(message))

    if item[1][0] and item[1][1] != "":
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
        print item[1][0].get('collect_time')
        print item[1][1].get('collect_time')
        if item[1][0].get('collect_time') - item[1][1].get('collect_time') > 2:
            print "time out in"
            update_msg(item[1][0], item[1][0].get('collect_time'))
    else:
        print "time out out"
        if item[1][0] == "" and item[1][1] != "":
            update_msg(item[1][1], item[1][1].get('collect_time'))
        elif item[1][1] == "" and item[1][0] != "":
            update_msg(item[1][0], item[1][0].get('collect_time'))
"""

# 对每个分区RDD操作
def foreachPartitionFun(rdd):
    def partitionOfRecordsFun(rdd):

        #topic = ConnectionPool.kafka_produce() 

        print "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        for item in itertools.chain(rdd):
            print item[0], item[1]
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            def update_msg(message, collect_time):
                collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(collect_time))
                change_dict = {'warning_state': 'event', 'warning_name': '断线', 'collect_time': collect_time}
                message.update(change_dict)

                # redis
                msgid = item[1][0].get('oid')
                RedisCache().set_data(msgid, message)
                RedisCache().lpush_data(message)
                print "redis His ok"


            if item[1][0] and item[1][1] != "":
                print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
                print item[1][0].get('collect_time')
                print item[1][1].get('collect_time')
                if item[1][0].get('collect_time') - item[1][1].get('collect_time') > 3:
                    print "time out in"
                    update_msg(item[1][0], item[1][0].get('collect_time'))
            else:
                print "time out out"
                if item[1][0] == "" and item[1][1] != "":
                    update_msg(item[1][1], item[1][1].get('collect_time'))
                elif item[1][1] == "" and item[1][0] != "":
                    update_msg(item[1][0], item[1][0].get('collect_time'))

    rdd.foreachPartition(partitionOfRecordsFun)


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding("utf-8")

    #checkpoint_path = "hdfs://spark-master:9000/checkpiont/streaming_cp_log"
    checkpoint_path = "tachyon-ft://spark-master:19998/checkpoint/streaming_log"
    kafka_topic_list = ["realdata_receive"]
    broker_list_dit = {"metadata.broker.list": "101.200.194.191:9092"}
    
    conf = SparkConf().setAppName("streaming_kafka_send")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    #setDefaultEncoding()
    #ssc = initStreamingContext("streaming_kafka_deltaT", "local[2]", 7)
    ssc.checkpoint(checkpoint_path)

    kvs = KafkaUtils.createDirectStream(ssc, kafka_topic_list, broker_list_dit)
    deltaT = kvs.flatMap(lambda lines: toJson(lines)).map(lambda x: (x["oid"], x)). \
        updateStateByKey(updateFun).foreachRDD(foreachPartitionFun)

    #ensureOffset(kvs=kvs)
    
    offsetRanges = []

    def storeOffsetRange(rdd):
        global offsetRanges
        offsetRanges = rdd.offsetRanges()
        return rdd
    def printOffsetRange(rdd):
        for o in offsetRanges:
            print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

    kvs.transform(storeOffsetRange).foreachRDD(printOffsetRange)

    ssc.start()
    ssc.awaitTermination()
