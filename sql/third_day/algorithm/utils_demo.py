#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'


def result_email(emailCollect, count):
    email_list = ['qq', '163', '126', 'sina', 'gmail', 'hotmail']
    email_collect = emailCollect
    other = 0
    for item in email_collect[0:-1]:
        if item[0].decode("utf-8") in email_list:
            print item[0], item[1], str("%.2f" % ((float(item[1]) / count) * 100)) + "%"
            email_collect.remove((item[0], item[1]))
    for item in email_collect:
        other += item[1]
    print "others", str(other), str("%.2f" % ((float(other) / count) * 100)) + "%"


def result_username_len(usernameLenCollect, count):
    level1 = level2 = level3 = level4 = 0
    for item in usernameLenCollect:
        if int(item[0]) > 0 and int(item[0]) <= 5:
            level1 += item[1]
        elif int(item[0]) > 0 and int(item[0]) <= 7:
            level2 += item[1]
        elif int(item[0]) > 0 and int(item[0]) <= 10:
            level3 += item[1]
        else:
            level4 += item[1]

    print "用户名字符长度："
    print "0-5: " + str(level1), "0-7: " + str(level2), "0-10: " + str(level3), "10+: " + str(level4)


def result_surname(surnameCollect, count):
    surnameList = surnameCollect
    other = 0
    for item in surnameList[0:20]:
        print item[0].encode("utf-8"), item[1], str("%.2f" % ((float(item[1]) / count) * 100)) + "%"
        surnameList.remove((item[0], item[1]))
    for item in surnameList:
        other += item[1]
    print "其他", str(other), str("%.2f" % ((float(other) / count) * 100)) + "%"


def result_realname(realnameLenCollect, count):
    lenList = ["2", "3"]
    realnameList = realnameLenCollect
    other = 0
    for item in realnameList[0:-1]:
        if item[0].encode("utf-8") in lenList:
            print item[0].encode("utf-8"), item[1], str("%.2f" % ((float(item[1]) / count) * 100)) + "%"
            realnameList.remove((item[0], item[1]))
    for item in realnameList:
        other += item[1]
    print "其他", str(other), str("%.2f" % ((float(other) / count) * 100)) + "%"


def result_privince(provinceCollect, count):
    pro_list = provinceCollect
    provinceList = {"11": "北京市", "50": "重庆市"}
    for item in pro_list[0:-1]:
        if provinceList.has_key(item[0].decode("utf-8")):
            print provinceList.get(item[0].decode("utf-8")), item[1], str(
                "%.2f" % ((float(item[1]) / count) * 100)) + "%"
