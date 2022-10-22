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

    #先用传统的sc.textFile加载目录的小文件，观察分区数
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_core/data/tiny_files')
    #再用优化的sc.wholeTextFiles加载目录的小文件，观察分区数
    rdd2=sc.wholeTextFiles('file:///export/pyworkspace/pyspark_dev/pyspark_core/data/tiny_files')
    print('rdd1的分区数=',rdd1.getNumPartitions())
    print('rdd2的分区数=',rdd2.getNumPartitions())

    sc.stop()

