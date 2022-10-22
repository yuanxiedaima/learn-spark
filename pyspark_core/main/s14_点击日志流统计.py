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
    conf=SparkConf().setAppName('test').setMaster('local[*]')
    sc=SparkContext(conf=conf)
    #加载文件生成RDD
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_core/data/access.log')
    #rdd1被复用多次，所以做持久化
    rdd1.persist(StorageLevel.MEMORY_AND_DISK_2)
    rdd2=rdd1.map(lambda x:x.split(' ')[0])
    #计算需求1：PV访问量
    pv=rdd2.count()
    print('PV访问量',pv)
    #计算需求2：UV访问量
    uv=rdd2.distinct().count()
    print('UV访问量', uv)
    #需求3：查询前10大网站
    #过滤掉无用记录
    rdd3=rdd1.filter(lambda x:len(x.split(' '))>=11 )
    #提取出网站，并对网站做wordcount词频统计
    result_list=rdd3.map(lambda x:x.split(' ')[10])\
        .map(lambda x:x[1:-1])\
        .filter(lambda x:x!='-')\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x+y)\
        .sortBy(lambda x:x[1],ascending=False)\
        .take(10)
    print('查询前10大网站:')
    for x in result_list:print(x)

    sc.stop()



