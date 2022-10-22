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
    #设置参数的，语法2
    spark.sql('set spark.sql.shuffle.partitions=4')
    #加载文件，用csv的格式形成DataFrame
    df1=spark.read\
        .option('sep','::')\
        .schema('user_id string,movie_id string,score int,timestamp long ')\
        .csv('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/ratings.dat')
    #df1.cache()
    df1.printSchema()
    df1.show()
    #先用SQL风格进行分析
    #获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
    df1.createOrReplaceTempView('movie_t')
    df2=spark.sql('''
        select movie_id,
               round(avg(score) , 2) as avg_score,
               count(user_id) as cnt
        from movie_t
        group by movie_id
        having cnt>2000
        order by avg_score desc 
        limit 10
    ''')
    df2.show()
    #再用DSL风格进行分析
    import pyspark.sql.functions as F

    df1.groupby('movie_id') \
        .agg(
            F.round(F.avg('score'), 2).alias('avg_score'),
            F.count('user_id').alias('cnt')
        ).where('cnt>2000') \
        .orderBy('avg_score', ascending=False) \
        .limit(10) \
        .show()
    #对结果保存为csv文件
    df2.write\
        .mode('overwrite')\
        .option('header',True)\
        .csv('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/out')
    #对结果保存到mysql，需要提前上传mysql的驱动jar包到spark的安装包目录中
    df2.write.jdbc(
        url='jdbc:mysql://node1:3306/bigdata',
        table='movie_top10',
        mode='overwrite',
        properties={'user':'root','password':'123456'}
    )

    time.sleep(20*60)
    spark.stop()
