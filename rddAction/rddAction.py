#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   rddAction.py
@Time    :   2019/8/11 21:53
@Desc    :

'''
from pyspark import SparkContext,SparkConf



if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("Transformaion")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1,2,3,4,5,6,7,2,4,5,1,1,1,5,8,9])

    print(rdd.collect())
    print(rdd.count())
    print(rdd.countByValue())
    print(rdd.take(10))
    print(rdd.top(5))
    print(rdd.reduce(lambda x,y:x+y))