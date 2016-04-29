#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

def result_email(emailCollect, count):
    other = 0
    for item in emailCollect:
        if item[0] == "qq":
            print item[0],item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        elif item[0] == "163":
            print item[0], item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        elif item[0] == "126":
            print item[0], item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        elif item[0] == "sina":
            print item[0], item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        elif item[0] == "gmail":
            print item[0], item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        elif item[0] == "hotmail":
            print item[0], item[1], str("%.2f" % ((float(item[1])/count)*100)) + "%"
        else:
            other += item[1]
    print "others", str(other), str("%.2f" % ((float(other)/count)*100)) + "%"