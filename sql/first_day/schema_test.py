#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
import os

if __name__ == "__main__":
    file_path = os.path.abspath("../doc/book.txt")
    print file_path

    conf = SparkConf().setAppName("schema_test").setMaster("local")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    lines = sc.textFile(file_path)
    # 切分
    parts = lines.map(lambda lines: lines.split(","))

    # 隐射表间关系(定义表结构)
    book = parts.map(lambda book: Row(name=book[0], author=book[1], price=float(book[2]), publish=book[3]))

    # 转换成schema并注册
    schemaPeople = sqlContext.createDataFrame(book)
    schemaPeople.registerTempTable("book")

    # 定义sqk语句(查询prize在50、60之间的书)
    book = sqlContext.sql("SELECT * FROM book WHERE price > 50.0 AND price < 60")

    # 查询结果进行隐射
    bookMap = book.map(lambda books: (books.name, books.author, books.price, books.publish))

    for book in bookMap.collect():
        print "|Name: " + book[0], "|Author: " + book[1], "|Price: " + str(book[2]), "|Publish: " + book[3] + "|"
