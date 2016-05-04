#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext, Row
from sql.third_day.algorithm.utils_demo import *
from datetime import datetime
import os


def analysis_email(email):
    """
    邮箱分割
    """
    return email.split("@")[1].split(".")[0]


if __name__ == "__main__":
    conf = SparkConf().setAppName("analysis_demo").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # UDF自定义函数注册
    sqlContext.registerFunction("analysis_email", analysis_email)

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
    email_str = "SELECT analysis_email(email) AS email FROM information"
    emailSQL = sqlContext.sql(email_str)
    # 求总数
    count = emailSQL.count()
    # 分组统计
    emailCollect = emailSQL.groupBy("email").count().collect()
    # email分析结果
    result_email(emailCollect, count)

    """
    :用户名与姓名分析与统计
    """
    """
    # 用户名长度统计
    username_len_str = "SELECT LENGTH(username) AS username_len FROM information"
    usernameSQL = sqlContext.sql(username_len_str)
    usernameLenCollect = usernameSQL.groupBy("username_len").count().collect()
    result_username_len(usernameLenCollect, count)

    # 姓氏统计
    surname_sql = "SELECT SUBSTRING(realname,0,1) AS surname FROM information"
    surnameSQL = sqlContext.sql(surname_sql)
    surnameCollect = sorted(surnameSQL.groupBy("surname").count().collect(), key=(lambda x: x[1]), reverse=True)
    result_surname(surnameCollect, count)

    # 姓名长度统计
    realname_sql = "SELECT LENGTH(realname) AS realname_len FROM information"
    realnameSQL = sqlContext.sql(realname_sql)
    realnameLenCollect = realnameSQL.groupBy("realname_len").count().collect()
    result_realname(realnameLenCollect, count)
    """

    """
    :身份证分析与统计
    """
    """
    # 身份证长度统计
    idcard_len_str = "SELECT LENGTH(idcard) AS idcard_len FROM information"
    idcardLenSQL = sqlContext.sql(idcard_len_str)
    temp_count = idcardLenSQL.groupBy("idcard_len").count()
    idcardLenCollect = temp_count.filter(temp_count["idcard_len"] == '18').collect()
    print idcardLenCollect
    #result_username_len(idcardLenCollect, count)
    """
    now_datetime = datetime.now()
    now_year = now_datetime.year

    idcard_str = "SELECT SUBSTRING(idcard,0,2) AS province_code,SUBSTRING(idcard,7,8) AS birthday, " \
                 "CAST(SUBSTRING(idcard,7,4) AS INT) AS birth_year,SUBSTRING(idcard,11,2) AS birth_month," \
                 "SUBSTRING(idcard,13,2) AS birth_day,SUBSTRING(idcard,17,1) AS gender," \
                 + str(now_year) + "-SUBSTRING(idcard,7,4) AS age " \
                 "FROM information WHERE LENGTH(idcard)='18'"

    idcardSQL = sqlContext.sql(idcard_str)

    provinceCollect = idcardSQL.groupBy("province_code").count().collect()
    result_privince(provinceCollect, count)

    birthdayCollect = idcardSQL.groupBy("birth_year").count().collect()
    print birthdayCollect

    ageCollect = idcardSQL.groupBy("age").count().collect()
    print ageCollect

    genderCollect = idcardSQL.groupBy("gender").count().collect()


    idcardSQL.show()

    schemaInfo.show()

    sc.stop()
