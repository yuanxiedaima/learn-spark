from pyspark.sql import SparkSession,Row
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
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #从高级别的SparkSession对象中获取低级别的SparkContext
    sc=spark.sparkContext
    #加载文件形成RDD1,每个元素是string
    rdd1=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.txt')
    rdd1.foreach(print)
    #将RDD1转换成RDD2，每个元素是Row对象
    rdd2=rdd1.map(lambda x: Row(name=x.split(', ')[0],age=int(x.split(', ')[1])) )
    #将RDD2转换成DataFrame
    dataframe=spark.createDataFrame(rdd2)
    #打印dataframe的schema表结果
    dataframe.printSchema()
    #打印dataframe的数据内容
    dataframe.show()

    #查询年龄>20岁的记录行，并显示
    dataframe.createTempView('people_t')
    dataframe2=spark.sql('select * from people_t where age>20')
    dataframe2.show()