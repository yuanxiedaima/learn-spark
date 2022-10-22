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
    #需要先设置检查点的HDFS目录
    sc.setCheckpointDir('hdfs://node1:8020/ckp')
    #加载txt文件形成RDD
    rdd=sc.textFile('file:///etc/profile')
    #先对rdd调用cache，将数据保存到内存
    rdd.cache()
    # 再将RDD数据保存到检查点目录中，它是延迟计算的，需要后面的action来触发
    # 直接从内存复制到HDFS即可，
    rdd.checkpoint()

    #立即调用count得到元素个数,可以触发上面的cache
    #rdd第一次的action会触发2个job，其中一个job会额外将内存数据复制到HDFS
    cnt1=rdd.count()
    print('第一次count结果',cnt1)
    #第二次调用count，预计这一次会更快。优先从内存获取数据，因为内存更快，如果内存没有，再去找HDFS拿数据
    cnt2=rdd.count()
    print('第二次count结果',cnt2)

    #故意将内存释放，下面的count，则只能退而求其次，选择HDFS拿数据
    rdd.unpersist() # 这句代码是立即执行
    cnt3=rdd.count()
    print('第三次count结果', cnt3)
    time.sleep(20*60)

    sc.stop()







