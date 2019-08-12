#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   persistion.py
@Time    :   2019/8/11 22:25
@Desc    :

'''

'''
使用持久化了，能提高效率
'''

from pyspark import SparkConf,SparkContext
import time

def timeWithNoPersist(sc):
    """
        Description:不适用持久化的时间
        Params:

        Return:

        Author:
                HY
        Modify:
                2019/8/11 22:32
    """
    time1=time.time()
    rdd=sc.textFile('data.txt')
    counts=[]
    for i in range(10):
        counts.append(rdd.count())
    time2=time.time()
    print('timeWithNoPersist is : %4.f'%(time2-time1))

def  timeWithPersist(sc):
    """
        Description:使用持久化的时间
        Params:

        Return:

        Author:
                HY
        Modify:
                2019/8/11 22:33
    """
    time1 = time.time()
    rdd = sc.textFile('data.txt').persist()
    counts = []
    for i in range(10):
        counts.append(rdd.count())
    time2 = time.time()
    print('timeWithPersist is : %4.f' % (time2 - time1))
    rdd.unpersist()


if __name__ == '__main__':
    conf=SparkConf().setMaster("local").setAppName("testPersist")
    sc=SparkContext(conf=conf)
    timeWithNoPersist(sc)
    timeWithPersist(sc)