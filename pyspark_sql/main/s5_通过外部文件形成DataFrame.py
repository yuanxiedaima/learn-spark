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
    # 创建上下文对象，先创建SparkSession，再获取SparkContext
    spark = SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #加载txt文件，形成DataFrame
    df1=spark.read.text('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.txt')
    #只有1个字段
    df1.printSchema()
    df1.show()
    #加载csv格式文件，形成DataFrame
    #option('header',True),意思是将第一行识别为表头，而不是记录
    #option('sep',';'),意思是用;分号去分隔列
    df2 = spark.read \
        .option('header', True) \
        .option('sep', ';') \
        .option('inferSchema', True) \
        .csv('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.csv')
    df2.printSchema()
    df2.show()
    #加载json格式文件，形成DataFrame
    df3=spark.read.json('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.json')
    df3.printSchema()
    df3.show()
    #加载parquet格式文件，形成DataFrame
    df4=spark.read.parquet('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/users.parquet')
    df4.printSchema()
    df4.show()
    spark.stop()