kinit -kt user.keytab user@XXX.COM
touch /tmp/$1.log
spark3-submit --py-files hdfs:///dlproc/dlproc_spark.py hdfs:///dlproc/dlproc_spark.py "$1" >> "/tmp/$1.log" 2>&1
hadoop fs -moveFromLocal -f /tmp/$1.log hdfs:///dlproc/logs
