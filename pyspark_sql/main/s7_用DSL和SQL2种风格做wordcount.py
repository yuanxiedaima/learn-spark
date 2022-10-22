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
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #加载文件形成DataFrame
    df1=spark.read.text('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/words.txt')
    df1.printSchema()
    df1.show()
    #用SQL风格做wordcount
    df1.createTempView('words_t')
    spark.sql('''
    select word,
           count(1) as cnt
    from 
    (select  explode(split(value,' ')) as word
    from words_t) as t
    group by word
    order by cnt desc
    ''').show()
    #用DSL风格做wordcount
    # 调用count()会自动增加一列count列名
    import pyspark.sql.functions as F
    df1.select(F.split('value', ' ').alias('arr')) \
        .select(F.explode('arr').alias('word')) \
        .groupby('word').count() \
        .orderBy('count', ascending=False)\
        .show()
