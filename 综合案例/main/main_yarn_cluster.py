import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import *

os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    # 下面【spark.sql.warehouse.dir】和【hive.metastore.warehouse.dir】选一个即可
    spark=SparkSession\
        .builder\
        .appName('test')\
        .config('spark.sql.warehouse.dir','hdfs://node1:8020/user/hive/warehouse/')\
        .config('hive.metastore.warehouse.dir','hdfs://node1:8020/user/hive/warehouse/')\
        .config('hive.metastore.uris','thrift://node1:9083')\
        .enableHiveSupport()\
        .getOrCreate()
    sc=spark.sparkContext
    sc.setLogLevel('WARN')
    #1加载user.txt，形成RDD1
    rdd1=sc.textFile('hdfs:///pydata/user.txt')
    rdd1=rdd1.map(lambda x:x.split(',')).map(lambda l:(int(l[0]),l[1],int(l[2]),l[3],l[4]))
    #2加载black_list.txt，形成RDD2，并collect形成list
    rdd2=sc.textFile('hdfs:///pydata/black_list.txt')
    rdd2=rdd2.map(lambda x:x.split(',')).map(lambda l:(int(l[0]),l[1]))
    black_list=rdd2.map(lambda x:x[0]).collect()
    #3定义广播变量，将list广播出去
    bc=sc.broadcast(black_list)
    #4定义累加器，碰到实际的黑名单用户就+1
    acc=sc.accumulator(0)
    #5在分布式代码中，也就是在Executor的task代码中获取广播变量list，
    #6对海量数据的局部数据的每条进行判断，如果存在于list中，则累加器+1，且后面要剔除。
    def filt_func(x):
        black_list2=bc.value
        if x[0] in black_list2:
            global acc
            acc+=1
            return False
        else : return True

    #7对RDD1进行filter过滤后，统计各地区的人数，比如('上海',888),('北京',786)。。
    rdd3=rdd1.filter(filt_func)
    rdd4=rdd3.map(lambda x:(x[4],1)).reduceByKey(lambda x,y:x+y)
    #8将上面的结果输出到HDFS的一个csv文本文件,RDD写出前的分区数就是创建子文件的个数

    rdd4.map(lambda x:x[0]+','+str(x[1])).coalesce(1).saveAsTextFile('hdfs:///pydata/final3')
    print('RDD版-统计各地区的人数，保存文件成功')
    print('RDD版-统计实际黑名单人数',acc.value)

    #9加载user.txt形成DataFrame1
    df1=spark.read\
        .schema('id int,name string,age int,sex string,city string')\
        .csv('hdfs:///pydata/user.txt')
    df1.show()
    #10加载black_list.txt形成DataFrame2
    df2=spark.read\
        .schema('id int,name string')\
        .csv('hdfs:///pydata/black_list.txt')
    df2.show()
    # 11 SQL风格，剔除黑名单人员后，统计各地区的人数
    df1.createOrReplaceTempView('user')
    df2.createOrReplaceTempView('black_list')
    spark.sql('select * from user').show()
    spark.sql('select * from black_list').show()
    print('SQL风格版-统计各地区的人数')
    spark.sql('''
        select city,
               count(u.id) as cnt
        from user as u 
        left join black_list as b on u.id=b.id
        where b.id is null
        group by city
    ''').show()
    # 12SQL风格统计实际有多少个黑名单用户
    print('SQL风格版-统计实际黑名单人数')
    spark.sql('''
    select count(u.id) as cnt
        from user as u 
        join black_list as b on u.id=b.id
    ''').show()

    # 13 DSL风格，剔除黑名单人员后，统计各地区的人数
    df2=df2.withColumn('id2',df2['id'])
    #扩展如何用不同的字段进行关联
    df1.join(df2, df1['id']==df2['id2'], 'left')
    df3=df1.join(df2,'id','left')\
        .where('id2 is null')\
        .groupby('city')\
        .count()\
        .coalesce(1)
    print('DSL风格版-统计各地区的人数')
    df3.show()

    # 13 DSL风格，实际有多少个黑名单用户
    df4=df1.join(df2,'id')
    black_cnt=df4.count()
    print('DSL风格版-统计实际黑名单人数',black_cnt)
    # df3进行缓存持久化
    df3.persist(StorageLevel.MEMORY_AND_DISK_2)
    # 将df3的数据进一步保存奥checkpoint检查点目录
    sc.setCheckpointDir('hdfs:///tmp/ckp/test')
    df3.checkpoint()
    # 14 保存到csv文件
    df3.write.mode('overwrite').option('header','true').csv('hdfs:///pydata/csv/')
    print('保存到csv文件成功.')
    # 14 保存到mysql表
    df3.write.format('jdbc')\
        .mode('overwrite')\
        .option('url','jdbc:mysql://node1:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true')\
        .option('user','root')\
        .option('password','123456')\
        .option('dbtable','bigdata.city_user_cnt')\
        .save()
    print('保存到mysql成功.')
    # 14 保存到hive表

    # <property>
    #     <name>hive.cli.print.header</name>
    #     <value>true</value>
    # </property>
    #
    # <property>
    #     <name>hive.resultset.use.unique.column.names</name>
    #     <value>false</value>
    # </property>

    df3.write.mode('overwrite').saveAsTable('bigdata.city_user_cnt')
    print('保存到hive表成功.')
    #方式2,用SQL语句的形式。
    df3.createOrReplaceTempView('city_user_cnt_tb')
    spark.sql('insert overwrite table bigdata.city_user_cnt select * from city_user_cnt_tb')
    print('保存到hive表成功.')
    spark.stop()
