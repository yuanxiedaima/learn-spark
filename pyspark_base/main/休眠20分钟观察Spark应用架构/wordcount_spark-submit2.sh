/export/server/spark/bin/spark-submit \
--master yarn \
--deploy-mode client \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--conf "spark.pyspark.driver.python=/root/anaconda3/bin/python3" \
--conf "spark.pyspark.python=/root/anaconda3/bin/python3" \
/export/pyworkspace/pyspark_dev/pyspark_base/main/wordcount2.py

