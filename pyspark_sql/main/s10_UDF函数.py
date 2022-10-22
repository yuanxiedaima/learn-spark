import string

from pyspark.sql import SparkSession
import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
from pyspark.sql.types import *

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

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    import pandas as pd
    df_pd = pd.DataFrame(
        data={'integers': [1, 2, 3],
              'floats': [-1.0, 0.6, 2.6],
              'integer_arrays': [[1, 2], [3, 4.6], [5, 6, 8, 9]]}
    )
    df = spark.createDataFrame(df_pd)
    df.printSchema()
    df.show()
    # 需求0:体会如何调用内置函数比如 pow
    import pyspark.sql.functions as F
    df.select(
        '*',
        F.pow('integers',3).alias('new_col')
    ).show()
    # 需求1:定义方式1-udf(lambda 匿名函数，返回数据类型)
    # 定义函数，计算一个数字的平方
    udf1=F.udf( lambda x:x**2 , IntegerType())
    df.select(
        '*',
         udf1('integers').alias('myint2')
    ).show()

    #需求2: 定义方式2 - udf(def函数, 返回数据类型)
    def square(x):return x**2
    udf2=F.udf(square, IntegerType())
    df.select(
        '*',
         udf2('integers').alias('myint2')
    ).show()
    #需求3: 定义方式3 - udf(lambda x: def函数，返回数据类型)
    udf3=F.udf(lambda x:square(x),IntegerType())
    df.select(
        '*',
         udf3('integers').alias('myint3')
    ).show()
    #需求4: 使用Python @ 注解方式
    @F.udf(returnType=IntegerType())
    def square(x): return x ** 2
    df.select(
        '*',
        square('integers').alias('myint4')
    ).show()

    #需求5: 验证1 - 如果预期结果是int类型，而实际结果是float，则显示为null
    df.select(
        '*',
        udf1('integers').alias('myint'),
        udf1('floats').alias('myfloats')
    ).show()
    #需求6: 验证2 - 如果预期结果是float类型，而实际结果是int，则显示为null
    udf4=F.udf(lambda x:x**2,FloatType())
    df.select(
        '*',
        udf4('integers').alias('myint'),
        udf4('floats').alias('myfloats')
    ).show()
    #需求7: 定义udf5，返回值类型是数组类型
    udf5=F.udf(lambda arr:[ x**2 for x in arr] ,ArrayType(FloatType()))
    df.select(
        '*',
        udf5('integer_arrays').alias('arr2')
    ).show(truncate=False)
    #需求8: udf的返回值类型是Tuple或混合输出类型
    #如下：有一个函数，输入一个数字，返回数字以及该数字对应字母表中的字母。
    #定义udf，返回类型用自定义的schema
    import string
    def numletter(x):return (x,string.ascii_letters[x])
    schema=StructType([
        StructField('num',IntegerType()),
        StructField('letter',StringType())
    ])
    udf6=F.udf(numletter , schema)
    df.select(
        '*',
        udf6('integers').alias('num_letter')
    ).show()

    #上面都是用的DSL风格调用udf，下面用SQL来调用udf
    #需求9: 用SQL风格使用udf
    #注册成SparkSQL中的函数名
    spark.udf.register( 'my_udf1' ,udf1)
    #注册临时表名
    df.createOrReplaceTempView('temp_table')
    spark.sql('''
        select *,
               my_udf1(integers) as new_col 
          from temp_table
    ''').show()
