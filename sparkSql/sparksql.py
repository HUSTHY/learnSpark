#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :    HY
@Software:   PyCharm
@File    :   sparksql.py
@Time    :   2019/8/13 10:46
@Desc    :

'''
from pyspark import Row
from pyspark.sql import SparkSession

def sparksql():
    spark = SparkSession.builder.appName("PythonWordCount").master("local").getOrCreate()
    sc = spark.sparkContext
    line = sc.textFile("person.txt").map(lambda x: x.split(' '))
    personRdd = line.map(lambda p: Row(id=p[0], name=p[1], age=int(p[2])))#Row()函数的作用
    personRdd_tmp = spark.createDataFrame(personRdd) #rdd 转化为 dataFrame
    # personRdd_tmp.show()

    line=sc.textFile("salaryinfo.txt").map(lambda x: x.split(' '))
    salaryrdd=line.map(lambda p: Row(id=p[0], type=p[1], salary=int(p[2])))#Row()函数的作用
    salaryrdd_tmp=spark.createDataFrame(salaryrdd)

    personRdd_tmp.registerTempTable("person")#注册临时表
    salaryrdd_tmp.registerTempTable("salary")
    r=spark.sql("select p.id, p.name, p.age,s.salary,s.type from person p left join salary s on p.id=s.id where p.age>27  Limit 2")#右连接,这个是DataFrame()，row是Dataframe的行
    for ele in r.collect():#遍历每一个行
        id=ele['id']#ele是一个row类型
        name=ele['name']
        age=ele['age']
        salary=ele['salary']
        print(id)
        print(name)
        print(age)
        print(salary)






if __name__ == '__main__':
    sparksql()