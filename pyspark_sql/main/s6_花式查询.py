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
    #加载文件形成RDD
    sc=spark.sparkContext
    rdd=sc.textFile('file:///export/pyworkspace/pyspark_dev/pyspark_sql/data/people.txt')

    #将RDD转换成DataFrame
    df=rdd.map(lambda x:(x.split(', ')[0],int(x.split(', ')[1]))).toDF(['name','age'])


    #用DSL风格来分析
    #1.查看DataFrame中的内容，通过调用show方法
    df.show()
    #2.查看DataFrame的Scheme信息
    df.printSchema()
    #3.第一种方式查看name字段数据
    df2=df.select('name')
    df2.show()
    #4.第二种方式查看name字段数据
    df.select(['name','age']).show()
    #5.第三种方式查看name和age字段数据
    df.select(df['name'],df['age']).show()
    #6.每个年龄加1
    #下面是错误写法
    #df.select('age+1').show()
    df.select(df['age']+1).show()
    df.select((df['age']+1).alias('new_age')).show()
    #7.过滤出年龄大于21的数据
    df.where('age>21').show()
    df.filter('age>21').show()
    #8.统计每个年龄有多少人
    #注意下面的count()，与RDD的count()名字一样，但是效果不一样
    df.groupBy('age').count().show()
    #用SQL风格来分析
    #将DataFrame注册成临时（视图）表
    df.createOrReplaceTempView('people')
    #1 查看DataFrame中的内容
    df5=spark.sql('select * from people')
    df5.show()
    #2 查看DataFrame的Scheme信息
    df5.printSchema()
    #3 查看name字段数据
    spark.sql('select name from people').show()
    #4 根据age排序的前两个人员信息
    spark.sql('select * from people order by age desc limit 2').show()
    #5 查询年龄大于25的人的信息
    spark.sql('select * from people where age>25').show()
    spark.stop()