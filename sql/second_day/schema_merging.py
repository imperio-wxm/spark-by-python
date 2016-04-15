#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
import os

if __name__ == "__main__":
    file_path = os.path.abspath("../doc")
    print

    conf = SparkConf().setMaster("local[2]").setAppName("schema_merging")
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)

    # 创建DataFrame
    df1 = sqlContext.createDataFrame(sc.parallelize(range(1, 6)).map(lambda i: Row(single=i, double=i * 2)))
    df1.write.parquet(file_path + "/key=1")

    df2 = sqlContext.createDataFrame(sc.parallelize(range(6, 11)).map(lambda i: Row(single=i, double=i * 3)))
    df2.write.parquet(file_path + "/key=2")

    df3 = sqlContext.read.option("mergeSchema", "true").parquet(file_path)
    df3.printSchema()

    df3.collect()

    sc.stop()
