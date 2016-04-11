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

    # Displays the content of the DataFrame to stdout
    df.show()
    df.select("name").show()
    df.select(df['name'], df['age'] + 1).show()
    df.filter(df['age'] > 21).show()
    df.groupBy("age").count().show()

    sc.stop()