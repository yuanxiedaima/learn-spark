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
    #创建一个list，包含一些数字
    list1=[1,2,3,4,5]
    #将list转换成rdd,可以手动指定5个分区，即5个并行度
    rdd=sc.parallelize(list1,5)
    rdd.coalesce()
    #查看rdd的分区数
    print('分区数=',rdd.getNumPartitions())
    #计算rdd的元素的和
    sum1=rdd.sum()
    #打印结果
    print('sum=',sum1)
    #关闭上下文对象
    sc.stop()