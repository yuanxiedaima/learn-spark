import time

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

    #读取mysql的数据，形成个DataFrame
    df=spark.read\
        .format('jdbc')\
        .option('url','jdbc:mysql://node1:3306/bigdata')\
        .option('dbtable','movie_top10')\
        .option('user','root')\
        .option('password','123456')\
        .load()

    df.printSchema()
    df.show()

    #将DataFrame保存为text格式文件
    import pyspark.sql.functions as F
    df.select(F.concat_ws(',','movie_id','avg_score','cnt').alias('col1'))\
        .write\
        .mode('overwrite')\
        .format('text')\
        .save('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/text')
    #上面可以简写为
    df.select(F.concat_ws(',','movie_id','avg_score','cnt').alias('col1'))\
        .write\
        .mode('overwrite')\
        .text('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/text')
    #将DataFrame保存为json格式文件
    df.write.mode('overwrite')\
        .format('json')\
        .save('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/json')
    #简写成
    df.write.mode('overwrite')\
        .json('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/json')
    #将DataFrame保存为csv格式文件
    df.write.mode('overwrite')\
        .option('header',True)\
        .csv('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/csv')
    #将DataFrame保存到mysql中
    df.write.format('jdbc')\
        .option('url','jdbc:mysql://node1:3306/bigdata')\
        .option('dbtable','movie_top10_2')\
        .option('user','root')\
        .option('password','123456')\
        .save()
    # 将DataFrame保存为parquet格式文件
    df.write.format('parquet').save('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/parquet')
    #Spark保存的默认格式就是parquet格式
    df.write.save('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/default')
    df.write.parquet('file:///export/pyworkspace/pyspark_dev/pyspark_sql/out/parquet2')
