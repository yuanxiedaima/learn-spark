from pyspark.sql import SparkSession
import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    #创建上下文对象，先创建SparkSession，再获取SparkContext
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    #加载文件形成RDD1
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.txt')
    rdd1.foreach(print)
    #将RDD1的元素从string转换成元组，形成RDD2
    rdd2=rdd1.map(lambda x:[x.split(', ')[0],int(x.split(', ')[1])])
    rdd2.foreach(print)
    dataframe=rdd2.toDF(['name','age'])
    #打印DataFrame的表结构
    dataframe.printSchema()
    #打印DataFrame的表记录行
    dataframe.show()

    spark.stop()