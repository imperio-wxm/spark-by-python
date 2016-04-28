#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
import time
from datetime import datetime,timedelta
from sql.utils.redis_conn import RedisCache
import itertools

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
    start_time = int(time.mktime((now_datetime - timedelta(seconds=5)).timetuple()))
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
        #sqlContext.cacheTable("his_data_zadd")

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

        # 结果存储parquet格式
        #assets.saveAsParquetFile("hdfs://localhost:9000/user/parquet_1")

        assets.show()

        # 查询结果进行隐射
        # assetMap = assets.map(lambda asset: (asset.name, asset.value)).foreachPartition(print_fun)
        #assetMap = assets.map(lambda asset: (asset.name, asset.value))
        #assetMap = assets.map(lambda asset: (asset.max_value))
        assetMap = assets.map(lambda asset: (asset.name, asset.oid, asset.value))
        collect_asset = assetMap.collect()

        print_fun(collect_asset)

    sc.stop()

