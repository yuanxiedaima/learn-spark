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
    #加载多个长句子，形成rdd
    lineseq = ["hadoop scala   hive spark scala sql sql",
               "hadoop scala spark hdfs hive    spark",
               "spark hdfs   spark hdfs scala hive spark"]
    rdd=sc.parallelize(lineseq)
    #进行扁平化->映射成键值对
    import re
    #下面的\s 表示一个空白字符，他包括空白，tab制表符，换行符等等，加号+表示多个
    rdd2=rdd.flatMap(lambda x:re.split('\s+',x))
    rdd3=rdd2.map(lambda x:(x,1))
    rdd3.foreach(print)
    #用groupyByKey进行wordcount
    rdd4=rdd3.groupByKey()
    rdd5 = rdd4.mapValues(lambda v:sum(v))
    rdd5=rdd4.mapValues(sum)
    print('groupByKey进行wordcount的结果')
    rdd5.foreach(print)

    #用aggregateByKey进行wordcount
    from operator import add
    rdd6=rdd3.aggregateByKey(0,add,add)
    print('aggregateByKey进行wordcount的结果')
    rdd6.foreach(print)
    #用foldByKey进行wordcount
    rdd7=rdd3.foldByKey(0,add)
    print('foldByKey进行wordcount的结果')
    rdd7.foreach(print)
    #用reduceByKey进行wordcount
    rdd8=rdd3.reduceByKey(add)
    print('reduceByKey进行wordcount的结果')
    rdd8.foreach(print)

    sc.stop()

