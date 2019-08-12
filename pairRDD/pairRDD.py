#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   pairRDD.py
@Time    :   2019/8/12 11:16
@Desc    :

'''
from pyspark import SparkContext,SparkConf
def createPairRDD(sc):
    rdd=sc.parallelize(['huangyagn 29 years old','huahzongkejidaxue wuhan gognshanqu', 'guangzhoushi tianhequ'])
    pairrdd=rdd.map(lambda x:(x.split(" ")[0],x))#使用map把第一个元素作为键值对的键,结果是元组列表
    print(pairrdd.collect())
def transforamtionPairRdd(sc):
    # rdd=sc.parallelize({(1,5),(3,9),(2,2),(6,0),(4,3)})
    rdd = sc.parallelize([(1, 5), (3, 9), (2, 2), (6, 0), (4, 3),(3, 3)]).persist()
    rdd1=rdd.reduceByKey(lambda x,y:x*y)#合并键相同的
    print(rdd1.collect())
    rdd2=rdd.groupByKey()
    for i in range(5):
        print(list(rdd2.collect()[i][1]))#collect()是一个ResultIterable结果集迭代对象,必须使用list强转，然后既可以打印了。
    rdd3=rdd.mapValues(lambda x:x*0.1) #操作每一个元素
    print(rdd3.collect())
    rdd4=rdd.flatMapValues(lambda x:range(x,7))#对每个元素给以一个可迭代的函数，来转化
    print(rdd4.collect())
    print(rdd.keys().collect())#获取键
    print(rdd.values().collect())#获取值
    print(rdd.sortByKey(ascending=False).collect())#键值对——键排序
    print(rdd.sortBy(keyfunc= lambda x: x[1],ascending=False).collect())#键值对排序——值排序
def ActionOperationRDD(sc):
    rdd = sc.parallelize([(1, 5), (3, 9), (2, 2), (6, 0), (4, 3), (3, 3)]).persist()
    r1=rdd.countByKey()#对每个键对应的元素分别计数
    print(r1[1])
    print(r1[6])
    r2=rdd.countByValue()#按照值，进行统计键值对
    print(r2)
    r3=rdd.collectAsMap()#将结果以映射表的形式返回，以便查询
    # 如果RDD中同一个Key中存在多个Value，那么后面的Value将会把前面的Value覆盖
    print(r3)
    r4=rdd.lookup(3)#返回给定键对应的所有值
    print(r4)



if __name__ == '__main__':
    conf=SparkConf().setMaster("local").setAppName("pairRDD")
    sc=SparkContext(conf=conf)
    # createPairRDD(sc)
    # transforamtionPairRdd(sc)
    ActionOperationRDD(sc)
