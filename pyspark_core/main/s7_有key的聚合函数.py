
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
    #全局指定默认的并行度
    conf=SparkConf().setAppName('test').setMaster('local[*]').set('spark.default.parallelism',4)
    sc=SparkContext(conf=conf)

    #创建rdd，有3个键值对
    rdd=sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    #用groupByKey调用，顺便计算wordcount
    rdd2=rdd.groupByKey()
    print('用groupByKey的结果--------')
    rdd2.mapValues(lambda v: sum(v)).foreach(print)
    #用aggregateByKey调用，顺便计算wordcount
    rdd3=rdd.aggregateByKey(0,lambda x,y:x+y, lambda x,y:x+y)
    from operator import add
    rdd3 = rdd.aggregateByKey(0, add, add)
    print('用aggregateByKey的结果--------')
    rdd3.foreach(print)

    rdd4 = rdd.aggregateByKey(1, lambda x, y: x + y, lambda x, y: x + y)
    print('用aggregateByKey的结果,当初始值是1时--------')
    rdd4.foreach(print)
    #用foldByKey调用，顺便计算wordcount
    #如果上面的aggregateByKey，的分区内和分区间的逻辑一样，则简化成foldByKey，
    rdd5 = rdd.foldByKey(0, add)
    print('用foldByKey的结果--------')
    rdd5.foreach(print)

    #用reduceByKey调用，顺便计算wordcount
    #如果上面的foldByKey的初始值没有意义时，则可以简化成reduceByKey
    rdd6=rdd.reduceByKey(add)
    print('用reduceByKey的结果--------')
    rdd6.foreach(print)