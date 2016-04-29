#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
from sql.third_day.algorithm.utils_demo import *
import os


def analysis_email(email):
    """
    邮箱分割
    """
    return email.split("@")[1].split(".")[0]


def analysis_username(username):
    """
    用户名长度计算
    """
    return len(username)


def analysis_surname(realname):
    """
    用户名姓氏分割
    """
    return realname[0]


if __name__ == "__main__":
    conf = SparkConf().setAppName("analysis_demo")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # UDF自定义函数注册
    sqlContext.registerFunction("analysis_email", analysis_email)
    sqlContext.registerFunction("analysis_username", analysis_username)
    sqlContext.registerFunction("analysis_surname", analysis_surname)

    file_path = os.path.abspath("../doc/analysis.txt")
    lines = sc.textFile(file_path)

    info = lines.map(lambda lines: lines.split("----")). \
        map(lambda info: Row(email=info[0], username=info[1], realname=info[2],
                             idcard=info[3], password=info[4], phone=info[5]))

    schemaInfo = sqlContext.createDataFrame(info)
    schemaInfo.registerTempTable("information")
    # cache表
    sqlContext.cacheTable("information")

    """
    :邮箱分析与统计
    """
    """
    email_str = "SELECT analysis_email(email) AS email FROM information"
    emailSQL = sqlContext.sql(email_str)
    # 求总数
    count = emailSQL.count()
    # 分组统计
    emailCollect = emailSQL.groupBy("email").count().collect()
    # email分析结果
    result_email(emailCollect, count)
    """


    """
    :用户名与姓名分析与统计
    """
    """
    # 用户名长度统计
    username_len_str = "SELECT analysis_username(username) AS username_len FROM information"
    usernameSQL = sqlContext.sql(username_len_str)
    usernameLenCollect = usernameSQL.groupBy("username_len").count().collect()
    result_username_len(usernameLenCollect, count)

    # 姓氏统计
    surname_sql = "SELECT analysis_surname(realname) AS surname FROM information"
    surnameSQL = sqlContext.sql(surname_sql)
    surnameCollect = sorted(surnameSQL.groupBy("surname").count().collect(), key=(lambda x: x[1]), reverse=True)
    result_surname(surnameCollect, count)

    # 姓名长度统计
    realname_sql = "SELECT analysis_username(realname) AS realname_len FROM information"
    realnameSQL = sqlContext.sql(realname_sql)
    realnameLenCollect = realnameSQL.groupBy("realname_len").count().collect()
    result_realname(realnameLenCollect, count)
    """

    """
    :身份证分析与统计
    """
    # 用户名长度统计
    idcard_len_str = "SELECT analysis_username(idcard) AS idcard_len FROM information"
    idcardSQL = sqlContext.sql(idcard_len_str)
    temp_count = idcardSQL.groupBy("idcard_len").count()
    idcardLenCollect = temp_count.filter(temp_count["idcard_len"] == '18').collect()
    print idcardLenCollect
    #result_username_len(idcardLenCollect, count)

    schemaInfo.show()

    sc.stop()
