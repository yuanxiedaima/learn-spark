/export/server/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 3 \
--executor-cores 1 \
--executor-memory 512M \
/export/pyworkspace/pyspark_dev/综合案例/main2/main_on_yarn.py