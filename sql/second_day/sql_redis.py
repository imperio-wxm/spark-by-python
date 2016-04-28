#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
import redis
import time
from datetime import datetime,timedelta
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
    def z_add(self, key, value, size):
        """
        value不能出现""，双引号
        """
        return self._connection.zadd(key, value, size)


def print_fun(collect_asset):
    for item in collect_asset:
        pass
        #print "|Name: " + item[0], "|Value: " + str(item[1])
        #RedisCache().z_add("save_his", item[0].encode("utf-8"), item[1])


if __name__ == "__main__":
    conf = SparkConf().setMaster("sql_redis").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # 查询从当前时间开始前10s的数据
    now_datetime = datetime.now()
    end_time = int(time.mktime(now_datetime.timetuple()))
    start_time = int(time.mktime((now_datetime - timedelta(minutes=250)).timetuple()))
    print start_time
    print end_time

    his_data = RedisCache().zrange_by_score("his_data_zadd", start_time, end_time)
    #print his_data['result']

    if his_data['result']:
        hisRDD = sc.parallelize(his_data['result'])
        his = sqlContext.jsonRDD(hisRDD)
        print his.count()


        row = his.sort(his.value.desc()).first()
        print "|Time: " + row[0], "|Name: " + row[1], "|Oid: " + row[2], "|Value: " + str(row[3]) + "|"

        his.registerTempTable("his_data_zadd")

        #assets = sqlContext.sql("SELECT his.name, his.oid FROM his_data_zadd as his WHERE his.value > 200 AND his.oid < 3000000")
        #sql_str = "SELECT his.name, his.oid FROM (SELECT MAX(temp_t.value), temp_t.name, temp_t.oid FROM his_data_zadd AS temp_t) his"
        #sql_str = "SELECT his.name, his.oid, his.value FROM his_data_zadd AS his ORDER BY his.value DESC LIMIT 10"
        #sql_str = 'SELECT his.name, his.oid FROM his_data_zadd AS his WHERE EXISTS (SELECT MAX(temp_t.value) FROM his_data_zadd AS temp_t)'
        """
        Spark 1.5 does not support subquery.
        """
        #sql_str = 'SELECT MAX(his_t.value) as max_value FROM his_data_zadd AS his_t GROUP BY his_t.name,his_t.oid,his_t.collect_time,his_t.value'
        sql_str = 'SELECT * FROM his_data_zadd his_t ORDER BY his_t.value DESC LIMIT 1'
        #sql_str = 'SELECT * FROM (SELECT his_t.value,his_t.name FROM his_data_zadd his_t WHERE his_t.value > 100)'
        assets = sqlContext.sql(sql_str)

        assets.show()

        # 查询结果进行隐射
        # assetMap = assets.map(lambda asset: (asset.name, asset.value)).foreachPartition(print_fun)
        #assetMap = assets.map(lambda asset: (asset.name, asset.value))
        #assetMap = assets.map(lambda asset: (asset.max_value))
        assetMap = assets.map(lambda asset: (asset.name, asset.oid, asset.value))
        collect_asset = assetMap.collect()

        print_fun(collect_asset)

    sc.stop()

