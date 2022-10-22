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

    #加载文件形成RDD，有2个分区
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_core/data/accumulator_broadcast_data.txt')
    #定义list，包含所有的特殊字符
    list_v=[",", ".", "!", "#", "$", "%"]
    #将上面的list，从Driver端广播到各个Executor端中
    bc=sc.broadcast(list_v)
    #定义累加器，后期在分布式task中，遇到特殊字符则累加1次
    acc=sc.accumulator(0)
    #RDD1进行扁平化切分成小单元，包括单词和特殊符号，形成RDD2
    import re
    rdd2=rdd1.flatMap(lambda x:re.split('\s+',x)).filter(lambda x:x!='')
    #对RDD2进行filter过滤，将特殊字符过滤掉，只留下单词进入RDD3
    #filter函数过滤时，一旦碰到特殊字符，则累加器加1
    def filt_func(x):
        global acc
        list2=bc.value
        #如果是特殊字符
        if x in list2:
            acc.add(1)
            # 或acc=acc+1
            # 或acc+=1
            return False
        else:
            return True
    #因为1行代码写不完，所以没用lambda表达式，这里写def函数，里面可以写多行代码
    rdd3=rdd2.filter(filt_func)
    #对RDD3进行wordcount词频统计，打印结果
    print('词频统计的结果是')
    rdd3.map(lambda x:(x,1))\
        .reduceByKey(lambda x,y:x+y)\
        .coalesce(1)\
        .sortBy(lambda x:x[1],ascending=False)\
        .foreach(print)
    #打印累加器的最终值。
    print('特殊字符的个数是',acc.value)

    sc.stop()