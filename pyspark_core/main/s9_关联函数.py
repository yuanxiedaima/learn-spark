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
    #创建rdd1，代表员工信息
    rdd1 = sc.parallelize([(1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhangliu")])
    #创建rdd2，代表员工的部门信息
    rdd2 = sc.parallelize([(1001, "sales"), (1002, "tech")])

    #rdd1 join rdd2，观察结果
    rdd3=rdd1.join(rdd2)
    rdd3.foreach(print)

    #rdd1 leftOuterJoin rdd2，观察结果
    rdd4=rdd1.leftOuterJoin(rdd2)
    rdd4.foreach(print)