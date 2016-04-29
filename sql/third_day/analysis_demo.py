#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
from sql.third_day.algorithm.utils_demo import result_email
import os

def analysis_email(email):
    """
    邮箱分割
    """
    return email.split("@")[1].split(".")[0]

if __name__ == "__main__":
    conf = SparkConf().setAppName("analysis_demo")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # UDF自定义函数注册
    sqlContext.registerFunction("analysis_email", analysis_email)

    file_path = os.path.abspath("../doc/analysis.txt")
    lines = sc.textFile(file_path)

    info = lines.map(lambda lines: lines.split("----")).\
        map(lambda info: Row(email=info[0], username=info[1], realname=info[2],
                                      idcard=info[3], password=info[4], phone=info[5]))

    schemaInfo = sqlContext.createDataFrame(info)
    schemaInfo.registerTempTable("information")
    # cache表
    sqlContext.cacheTable("information")

    """
    :邮箱分析与统计
    """
    email_str = "SELECT analysis_email(email) AS email FROM information"
    emailSQL = sqlContext.sql(email_str)
    # 求总数
    count = emailSQL.count()
    # 分组统计
    emailCollect = emailSQL.groupBy("email").count().collect()
    # email分析结果
    result_email(emailCollect, count)

    sc.stop()
