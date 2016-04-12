#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
from pyspark.sql.types import *
import os

if __name__ == "__main__":
    json_path = os.path.abspath("../doc/book.json")
    txt_path = os.path.abspath("../doc/book.txt")

    conf = SparkConf().setAppName("mul_sources").setMaster("local[2]")
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    # json读取并隐射
    json_df = sqlContext.read.json(json_path)
    json_df.registerTempTable("json_book")

    # txt读取并隐射
    lines = sc.textFile(txt_path)
    parts = lines.map(lambda lines: lines.split(","))
    book = parts.map(lambda book: Row(name=book[0], author=book[1], price=float(book[2]), publish=book[3]))
    schemaPeople = sqlContext.createDataFrame(book)
    schemaPeople.registerTempTable("txt_book")

    # sql_book = sqlContext.sql("SELECT * FROM json_book AS jbook LEFT JOIN txt_book AS tbook ON tbook.name=jbook.name")
    sql_book = sqlContext.sql("SELECT * FROM json_book AS jbook , txt_book AS tbook "
                              "WHERE jbook.name=tbook.name ORDER BY tbook.price")

    bookMap = sql_book.map(lambda books: (books.name, books.author, books.price, books.publish, books.place))

    for book in bookMap.collect():
        print book[0], book[1], book[2], book[3], book[4]
