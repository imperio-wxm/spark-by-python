#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from sql.utils.redis_conn import RedisCache
from datetime import datetime
import time
import random

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

        #if count > 5000000:
            #flag = False

        time.sleep(1)

