from pyspark.sql import SparkSession
import pandas as pd
import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

if __name__ == '__main__':
    # 创建上下文对象，先创建SparkSession，再获取SparkContext
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #创建pandas的DataFrame，
    pdf = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["张大仙", '王晓晓', '王大锤'],
        "age": [11, 11, 11]
    })
    #创建Spark的DataFrame
    dataframe=spark.createDataFrame(pdf)
    #打印schema表结构信息
    dataframe.printSchema()
    #打印记录行信息
    dataframe.show()
    spark.stop()
