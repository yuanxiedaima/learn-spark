
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

    #创建一个RDD，包含几个数字
    rdd1=sc.parallelize([1,2,3,4,5,6],3)

    #使用aggregate 对rdd求和
    s1=rdd1.aggregate( 0 ,lambda x,y:x+y, lambda x,y:x+y)
    from operator import add
    #写法2
    s2 = rdd1.aggregate(0, add, add)
    print('aggregate求和结果',s1,s2)

    #使用fold 对rdd求和，如果aggregate的分区内和分区间的聚合函数逻辑一样时，可以简化成fold
    s3 = rdd1.fold(0, lambda x, y: x + y)
    print('fold求和结果', s3)
    #使用reduce 对rdd求和，如果上面的fold的初始值，没有意义时，可以省略
    s4 = rdd1.reduce( lambda x, y: x + y)
    print('reduce求和结果', s4)


