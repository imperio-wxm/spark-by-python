#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import os

if __name__ == "__main__":
    file_path = os.path.abspath("../doc/people.json")
    print file_path

    conf = SparkConf().setAppName("sql_test").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.json(file_path)

    """
    In Python it’s possible to access a DataFrame’s columns either by attribute (df.age) or by indexing (df['age']).
    While the former is convenient for interactive data exploration, users are highly encouraged to use the latter form,
    which is future proof and won’t break with column names that are also attributes on the DataFrame class.
    """

    # Displays the content of the DataFrame to stdout
    # 显示全表
    df.show()
    # 显示姓名
    df.select("name").show()
    # 显示姓名、年龄+1
    df.select(df['name'], df['age'] + 1).show()
    # 显示年龄>21
    df.filter(df['age'] > 21).show()
    # 按age分组计数
    df.groupBy("age").count().show()

    sc.stop()