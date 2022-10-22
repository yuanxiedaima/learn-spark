import string

from pyspark import StorageLevel
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
    spark=SparkSession.builder\
        .appName('test')\
        .master('yarn')\
        .config('hive.metastore.warehouse.dir','hdfs://node1:8020/user/hive/warehouse')\
        .config('hive.metastore.uris','thrift://node1:9083')\
        .enableHiveSupport()\
        .getOrCreate()
    sc=spark.sparkContext
    sc.setCheckpointDir('hdfs://node1:8020/ckp/')
    #1加载user.txt，形成RDD1
    rdd1=sc.textFile('hdfs://node1:8020/pydata/user.txt')
    user_rdd=rdd1\
        .map(lambda x:x.split(','))\
        .map(lambda x:(int(x[0]),x[1],int(x[2]),x[3],x[4]))
    #2加载black_list.txt，形成RDD2，并collect形成list
    rdd2 = sc.textFile('hdfs://node1:8020/pydata/black_list.txt')
    #当数据量小时，可以用collect()，数据量太大，则不建议用。
    black_list=rdd2.map(lambda x:int(x.split(',')[0])).collect()
    #3定义广播变量，将list广播出去
    bc=sc.broadcast(black_list)
    #4定义累加器，碰到实际的黑名单用户就+1
    acc=sc.accumulator(0)
    # 6对海量数据的局部数据的每条进行判断，如果存在于list中，则累加器+1，且后面要剔除。
    def filter_func(x):
        black_list2=bc.value
        if x[0] in black_list2:
            global acc
            acc+=1
            return False
        else: return True
    #5在分布式代码中，也就是在Executor的task代码中获取广播变量list，
    filtered_rdd=user_rdd.filter(filter_func)
    #7对RDD1进行filter过滤后，统计各地区的人数，比如('上海',888),('北京',786)。。
    ret_rdd1=filtered_rdd.map(lambda tup:(tup[4],1)).reduceByKey(lambda x,y:x+y)
    print('filter过滤后，统计各地区的人数:')
    ret_rdd1.foreach(print)
    #8将上面的结果输出到HDFS的一个csv文本文件
    ret_rdd1.coalesce(1).saveAsTextFile('hdfs://node1:8020/pydata/csv3')

    #9加载user.txt形成DataFrame1
    df1=spark.read\
        .schema('id int,name string,age int,sex string,city string')\
        .csv('hdfs://node1:8020/pydata/user.txt')
    df1.printSchema()
    df1.show()
    #10加载black_list.txt形成DataFrame2
    df2=spark.read\
        .schema('id int,name string')\
        .csv('hdfs://node1:8020/pydata/black_list.txt')
    df2.printSchema()
    df2.show()
    #11 SQL风格 统计实际有多少个黑名单用户
    df1.createOrReplaceTempView('users')
    df2.createOrReplaceTempView('black_list')
    spark.sql('set spark.sql.shuffle.partitions=4')
    print('SQL风格 统计实际有多少个黑名单用户')
    spark.sql('''
    select count(1) as cnt from users u join black_list b on u.id=b.id
    ''').show()
    #12 SQL风格,剔除掉黑名单用户之后，对剩下的用户统计各地区的人数

    print('SQL风格,剔除掉黑名单用户之后，对剩下的用户统计各地区的人数')
    spark.sql('''
        select city,
               count(u.id) as cnt
        from users u
        left join black_list b on u.id=b.id
        where b.id is null
        group by city
    ''').show()

    # 13 DSL风格 统计实际有多少个黑名单用户
    df3=df1.join(df2,[df1.id==df2.id],'inner')
    black_cnt=df3.count()
    print('DSL风格 统计实际有多少个黑名单用户',black_cnt)

    # 14 DSL风格,剔除掉黑名单用户之后，对剩下的用户统计各地区的人数
    df4=df1.alias('u').join(df2.alias('b'),'id','left').where('b.id is null').groupby('city').count().coalesce(1)
    #由于df4被使用了多次，可以提前缓存持久化
    df4.persist(StorageLevel.MEMORY_AND_DISK_2)
    df4.checkpoint()
    print('DSL风格,剔除掉黑名单用户之后，对剩下的用户统计各地区的人数')
    df4.show()

    #15-对SparkSQL的的结果，不光保存为csv，还要保存到mysql中。
    df4.coalesce(1).write.mode('overwrite').option('header',True).csv('hdfs://node1:8020/pydata/csv3')
    print('保存到csv。。')
    df4.coalesce(1).write\
        .mode('overwrite')\
        .format('jdbc')\
        .option('url','jdbc:mysql://node1:3306/bigdata?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true')\
        .option('dbtable','bank_report2')\
        .option('user','root')\
        .option('password','123456')\
        .save()
    print('保存到mysql。。')
    #16还要保存到hive中。
    spark.sql('drop table if exists bigdata.bank_report3')
    df4.coalesce(1).write.saveAsTable('bigdata.bank_report3')
    #还可以用SQL来插入hive
    df4.createOrReplaceTempView('bank_report_temp')
    # sql中的hint语法  /*+ repartition(1) */
    spark.sql('create table bigdata.bank_report4 as select /*+ repartition(1) */ * from bank_report_temp')
    print('保存到hive。。')
    #17-程序最后放在yarn上运行。