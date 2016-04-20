#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
import redis
import itertools

class RedisDBConfig:
    HOST = '192.168.108.222'
    PORT = 6379
    DBID = 0

def operator_status(func):
    def gen_status(*args, **kwargs):
        error, result = None, None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error = str(e)

        return {'result': result, 'error': error}

    return gen_status


class RedisCache(object):
    def __init__(self):
        if not hasattr(RedisCache, 'pool'):
            RedisCache.create_pool()
        self._connection = redis.Redis(connection_pool=RedisCache.pool)

    @staticmethod
    def create_pool():
        RedisCache.pool = redis.ConnectionPool(
            host=RedisDBConfig.HOST,
            port=RedisDBConfig.PORT,
            db=RedisDBConfig.DBID)

    @operator_status
    def set_data(self, key, value):
        return self._connection.set(key, value)


    @operator_status
    def zrange_by_score(self, key, start, end):
        return self._connection.zrangebyscore(key, start, end)

    @operator_status
    def z_add(self, key, size, value):
        return self._connection.zadd(key, size, value)

def print_fun(collect):
    for item in itertools.chain(collect):
        print "|Name: " + item[0], "|Value: " + str(item[1])
        RedisCache().zadd("save_his", item[1], item[0])

if __name__ == "__main__":
    conf = SparkConf().setMaster("sql_redis").setMaster("local")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    his_data = RedisCache().zrange_by_score("his_data", 123, 456)
    print his_data['result']
    RedisCache().z_add("testd", 123456 ,"dsfasd")

    hisRDD = sc.parallelize(his_data['result'])

    his = sqlContext.jsonRDD(hisRDD)
    his.registerTempTable("his_data")

    assets = sqlContext.sql("SELECT * FROM his_data")

    assets.show()

    # 查询结果进行隐射
    #assetMap = assets.map(lambda asset: (asset.name, asset.value)).foreachPartition(print_fun)
    assetMap = assets.map(lambda asset: (asset.name, asset.value))

    for item in assetMap.collect():
        print "|Name: " + item[0], "|Value: " + str(item[1])
        print type(item[0].encode("utf-8")), type(item[1])
        RedisCache().z_add("save_his", item[1], item[0].encode("utf-8"))

    sc.stop()
