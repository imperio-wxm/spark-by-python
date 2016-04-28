#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
import os

# 根据evaluation进行分类
def name_place(name, place, price, evaluation):
    if evaluation <= 3:
        return name + " general"
    elif evaluation > 3 and evaluation <=5:
        return name + " good"


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[2]").setAppName("sql_udf")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    json_path = os.path.abspath("../doc/book.json")

    # json读取并隐射
    json_df = sqlContext.read.json(json_path)
    json_df.registerTempTable("json_book")

    # UDF自定义函数
    sqlContext.registerFunction("name_place", name_place)

    lengthRDD = sqlContext.sql("SELECT name_place(name, place, price,evaluation) AS name_place FROM json_book")

    #bookMap = lengthRDD.map(lambda books: (books.name, books.author, books.price, books.publish, books.place))

    lengthRDD.show()

    sc.stop()