#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import itertools
# from default_utils import setDefaultEncoding, initStreamingContext, ensureOffset
import json
import datetime
import time
from pykafka import KafkaClient
import re
import Queue
import types
from contextlib import contextmanager

# 字符串多次替换
def multiple_replace(text, adict):
    rx = re.compile('|'.join(map(re.escape, adict)))

    def one_xlat(match):
        return adict[match.group(0)]

    return rx.sub(one_xlat, text)

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


class KafkaConfig:
    HOST = "192.168.108.222:9092"
    TOPIC = "disconnection_receive_topic"

class producer_message(object):
    def make_conn(self):
        client = KafkaClient(hosts=KafkaConfig.HOST)
        topic = client.topics[KafkaConfig.TOPIC]
        producer = topic.get_sync_producer()
        return producer

# 对象池
class ObjectPool(object):
    def __init__(self, fn_cls, *args, **kwargs):
        super(ObjectPool, self).__init__()
        self.fn_cls = fn_cls
        self._myinit(*args, **kwargs)

    def _myinit(self, *args, **kwargs):
        self.args = args
        self.maxSize = int(kwargs.get("maxSize", 1))
        self.queue = Queue.Queue()

    def _get_obj(self):
        # 因为传进来的可能是函数，还可能是类
        if type(self.fn_cls) == types.FunctionType:
            return self.fn_cls(self.args)
        # 判断是经典或者新类
        elif type(self.fn_cls) == types.ClassType or type(self.fn_cls) == types.TypeType:
            return apply(self.fn_cls, self.args)
        else:
            raise "Wrong type"

    def borrow_obj(self):
        # 这个print 没用，只是在你执行的时候告诉你目前的队列数，让你发现对象池的作用
        print self.queue._qsize()
        # 要是对象池大小还没有超过设置的最大数，可以继续放进去新对象
        if self.queue.qsize() < self.maxSize and self.queue.empty():
            self.queue.put(self._get_obj())
        # 都会返回一个对象给相关去用
        return self.queue.get()

    # 回收
    def recover_obj(self, obj):
        self.queue.put(obj)

# 不用构造含有__enter__, __exit__的类就可以使用with，当然你可以直接把代码放到函数去用
@contextmanager
def createKafkaProducerPool(pool):
    obj = pool.borrow_obj()
    try:
        yield obj
    except Exception, e:
        yield None
    finally:
        pool.recover_obj(obj)

# 对每个分区RDD操作
def foreachPartitionFun(rdd):
    def partitionOfRecordsFun(rdd):
        replace_dict = {"u'": "\"", "'": "\""}

        print "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        for item in itertools.chain(rdd):
            print item[0], item[1]
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

            def update_msg(message, collect_time):
                collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(collect_time))
                change_dict = {u'warning_state': u'event', u'warning_name': u'断线', u'collect_time': collect_time}
                message.update(change_dict)
                # 单引号转换
                message = multiple_replace(json.dumps(message), replace_dict)

                obj = ObjectPool(producer_message,maxSize=4)

                with createKafkaProducerPool(obj) as producer_obj:
                    producer_obj.make_conn().produce(message)
                print "kafka is ok"

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
    # checkpoint_path = "hdfs://spark-master:9000/checkpiont/streaming_cp_log"
    checkpoint_path = "tachyon-ft://spark-master:19998/checkpoint/streaming_log"
    kafka_topic_list = ["realdata_receive"]
    broker_list_dit = {"metadata.broker.list": "192.168.108.222:9092"}

    conf = SparkConf().setAppName("streaming_kafka_send")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    # setDefaultEncoding()
    # ssc = initStreamingContext("streaming_kafka_deltaT", "local[2]", 7)
    ssc.checkpoint(checkpoint_path)

    kvs = KafkaUtils.createDirectStream(ssc, kafka_topic_list, broker_list_dit)
    deltaT = kvs.flatMap(lambda lines: toJson(lines)).map(lambda x: (x["oid"], x)). \
        updateStateByKey(updateFun).foreachRDD(foreachPartitionFun)

    # ensureOffset(kvs=kvs)

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
