import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
import sys

from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    #创建上下文对象
    conf=SparkConf().setAppName('test').setMaster('local[*]')
    sc=SparkContext(conf=conf)

    #创建list，包含多个单词
    list1=["hello", "world", "hello", "world"]
    #将list转换成rdd
    rdd=sc.parallelize(list1,3)

    #对rdd做wordcount，得到词频统计结果
    wordcount_rd=rdd.map(lambda x:(x,1))\
       .reduceByKey(lambda x,y:x+y)

    #打印结果
    wordcount_rd.foreach(lambda x:print(x))
    sc.stop()