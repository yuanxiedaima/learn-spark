import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
import sys
import time

from pyspark import SparkConf, SparkContext, StorageLevel

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
    #加载源数据文件
    rdd1 = sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_core/data/SogouQ.reduced')
    import re
    #对文件进行过滤，清洗，转换
    rdd2 = rdd1.filter(lambda x: len(x) > 0 and len(re.split('\s+', x)) == 6)
    rdd3 = rdd2.map(lambda x: re.split('\s+', x))
    rdd4 = rdd3.map(lambda x: (x[0], x[1], x[2][1:-1], int(x[3]), int(x[4]), x[5]))
    rdd4.persist(StorageLevel.MEMORY_AND_DISK)
    #做需求1：搜索关键词统计
    import jieba
    list1_10=rdd4.map(lambda x:x[2])\
        .flatMap(lambda x:list(jieba.cut(x)))\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x+y)\
        .sortBy( lambda x:x[1] , ascending=False )\
        .take(10)
    print('搜索关键词统计的前10名结果是')
    for x in  list1_10: print(x)
    #做需求2:用户搜索点击统计
    list2_10=rdd4.map(lambda x:x[1]+'_'+x[2])\
        .map(lambda x:(x,1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], ascending=False) \
        .take(10)
    print('用户搜索点击统计的前10名结果是')
    for x in  list2_10: print(x)

    #需求3：搜索时间段统计
    print('搜索时间段统计的结果是')
    rdd4.map(lambda x:x[0][0:2])\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], ascending=False)\
        .foreach(print)
    sc.stop()