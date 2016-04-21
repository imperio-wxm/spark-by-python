#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

import redis
from datetime import datetime
import time
import random

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


if __name__ == "__main__":
    """
    生成模拟数据
    """
    flag = True
    #print timeStamp
    count = 0

    """
    '{"oid":"1392381","value":' + str(1) + ',"collect_time":"' + datetime.datetime.now().strftime(
        "%H:%M:%S:%f") + '","warring_state":"warning","warring_name":"属性001警告001"}'
    """

    while flag:
        now_datetime = datetime.now()
        timeStamp = int(time.mktime(now_datetime.timetuple()))

        oid = random.randint(1000000, 9999999)
        value = random.uniform(0, 500)

        message = '{"name":"asset' + str(count) +'","oid":"' + str(oid) + '","value":' + \
                  str(value) + ',"collect_time":"' + now_datetime.strftime("%H:%M:%S:%f") + '"}'
        count = count + 1
        RedisCache()._connection.zadd("his_data_zadd", message, timeStamp)

        print message
        #if count == 100001:
            #flag = False
        time.sleep(1)
