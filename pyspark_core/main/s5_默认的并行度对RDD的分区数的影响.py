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
    list1=[1,2,3,4,5]
    rdd1=sc.parallelize(list1)
    #如果上面指定了spark.default.parallelism=4，则下面rdd分区数默认就是4
    #否则如果不指定，则local[*]的*是几，则下面rdd分区数默认就是几
    print('rdd1的分区数=',rdd1.getNumPartitions())

    sc.stop()


