import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON


if __name__ == '__main__':
    #1-创建Spark上下文对象sc
    conf=SparkConf().setAppName('wordcount').setMaster('local[*]')
    sc=SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    #2-用sc加载linux本地文件，形成rdd1
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_base/data/words.txt')
    print('rdd1的每个元素是--------')
    rdd1.foreach(lambda x:print(x))
    #3-对rdd1的元素，从长句子，扁平化成为短单词，形成rdd2
    rdd2=rdd1.flatMap(lambda x:x.split(' '))
    print('rdd2的每个元素是--------')
    rdd2.foreach(lambda x:print(x))
    #4-对rdd2的元素，映射变换成键值对，形成rdd3
    rdd3=rdd2.map(lambda x:(x,1))
    print('rdd3的每个元素是--------')
    rdd3.foreach(lambda x:print(x))
    #5-对rdd3的元素，按照相同key的元素，分到一个子分区中，进行聚合，形成rdd4，得到结果
    #假设有3个,('dog',1),('dog',1),('dog',1)
    #reduceByKey,其实是先bykey，再reduce
    #先bykey，变成了('dog' , [1,1,1] )
    #再reduce,只对[1,1,1]进行reduce，【lambda x,y : x+y】意思是两两相加。得到3
    #最后得到('dog' , 3 )
    rdd4=rdd3.reduceByKey(lambda x,y:x+y)
    #6-打印rdd4的结果
    print('rdd4的每个元素是--------')
    rdd4.foreach(lambda x:print(x))
    import time
    time.sleep(20*60)
    #7-关闭上下文对象
    sc.stop()