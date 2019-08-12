#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   transformation.py
@Time    :   2019/8/11 11:33
@Desc    :

'''

from pyspark import SparkConf,SparkContext

def MAP(rdd):#操作每个元素
    r=rdd.map(lambda x:int(x)*int(x))#这里读出来的就是str 要做类型转化
    print(r.collect())
def Filter(rdd):#过滤
    r=rdd.filter(lambda x:int(x)>2)
    print(r.collect())

def MapFlat(rdd):
    r=rdd.flatMap(lambda line:line.strip().split(' '))#每一行是一个str然后str用空格分开
    print(r.collect())

def distinct(rdd):#去重
    r=rdd.distinct()
    print(r.collect())
    print(r.count())

def differenceofMAPandFlatMap(rdd):#map和flatMap的不同
    r1=rdd.map(lambda line:line.strip().split(' '))
    r2=rdd.flatMap(lambda line:line.strip().split(' '))
    print(r1.count())
    print(r1.collect())
    print(r2.count())
    print(r2.collect())

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("Transformaion")
    sc=SparkContext(conf=conf)
    rdd=sc.textFile('data.txt')
    # MAP(rdd)
    # Filter(rdd)
    # strrdd=sc.textFile('str.txt')
    # MapFlat(strrdd)
    # differenceofMAPandFlatMap(strrdd)
    # distinct(rdd)

    l1=[1,2,3,4]
    l2=[5,6,7,8]
    r1=sc.parallelize(l1)
    print(r1.collect())
    r2=sc.parallelize(l2)
    r1=r1.union(r2)  #伪集合操作
    print(r1.collect())