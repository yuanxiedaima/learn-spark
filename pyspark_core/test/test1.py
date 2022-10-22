
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
    #创建上下文对象
    spark=SparkSession.builder\
        .appName('test')\
        .master('local[*]')\
        .config('spark.sql.shuffle.partitions',4)\
        .getOrCreate()
    df=spark.sql("select 1 id,'zhangsan' name")
    from pyspark.sql.functions import *
    df.select(
        '*',
        substring('name',0,5).alias('newname')
    ).show()