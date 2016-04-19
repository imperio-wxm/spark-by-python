#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
import itertools

def print_fun(collect):
    for item in itertools.chain(collect):
        print "|Name: " + item[0], "|Value: " + str(item[1]), "|Attribute: " + item[2]

if __name__ == "__main__":
    conf = SparkConf().setAppName("json_ops").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    # 将json格式直接直接parallelize为RDD
    equipmentRDD = sc.parallelize(['{"name":"asset1","value":1,"attribute":"属性1"}',
                                   '{"name":"asset2","value":2,"attribute":"属性2"}',
                                   '{"name":"asset3","value":3,"attribute":"属性3"}'])

    equipment = sqlContext.jsonRDD(equipmentRDD)
    equipment.registerTempTable("equipment")

    assets = sqlContext.sql("SELECT * FROM equipment as eq WHERE eq.value >= 1 AND eq.value <= 2")

    assets.show()

    # 查询结果进行隐射
    assetMap = assets.map(lambda asset: (asset.name, asset.value, asset.attribute)).foreachPartition(print_fun)

    sc.stop()
