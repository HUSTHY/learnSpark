#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   firstspark.py
@Time    :   2019/8/10 10:49
@Desc    :

'''
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)
    print(sc)
    lines=sc.textFile("textFiles/README.md")
    print(lines.count())
    print(lines.collect())