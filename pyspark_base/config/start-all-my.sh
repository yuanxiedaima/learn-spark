source /etc/profile
echo '1/7,关闭hadoop..'
stop-all.sh
echo '2/7,关闭hiveserver2、metastore、SparkThriftServer'
jps -m | grep -E 'RunJar.*HiveServer2|RunJar.*HiveMetaStore|HiveThriftServer2'
y=`jps -m | grep -E 'RunJar.*HiveServer2|RunJar.*HiveMetaStore|HiveThriftServer2' | awk -F' ' '{print $1}'`
for x in $y
do
  echo 关闭进程号$x
  kill -9 $x
done

echo '3/7,启动hadoop..'
start-all.sh

echo '4/7,退出hdfs安全模式..'
hdfs dfsadmin -safemode forceExit

echo '5/7,启动hive-metastore..'

nohup /export/server/hive/bin/hive --service metastore > /tmp/hive-metastore.log 2>&1 &
echo '6/7,启动hive-hiveserver2..'
nohup /export/server/hive/bin/hive --service hiveserver2 > /tmp/hive-hiveserver2.log 2>&1 &

echo '7/7.启动spark-thriftserver..'
/export/server/spark/sbin/start-thriftserver.sh \
 --hiveconf hive.server2.thrift.port=10001 \
 --hiveconf hive.server2.thrift.bind.host=node1 \
 --master local[*]