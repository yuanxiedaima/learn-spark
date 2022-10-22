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
    #加载一个txt文件，形成RDD，指定一个【建议的最小分区数】
    rdd=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_base/data/words.txt',4)
    #做wordcount，得到结果RDD
    wordcount_rdd=rdd.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
    #打印RDD的结果,wordcount_list是python的list类型，而不是RDD
    wordcount_list=wordcount_rdd.collect()
    for x in wordcount_list:print(x)

    sc.stop()