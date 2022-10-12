# ads-udf
User defined function used in Hive/... for Ads

## Deploy
1. Copy the jar to some machine that may acccess S3: 
``` shell 
scp target/ads-hive-udfs-1.0.0.jar airflow-dev:~/jiajinyu/ 
```
2. Copy the jar from local to S3
``` shell 
hadoop fs -put ads-hive-udfs-1.0.0.jar s3a://newsbreak-monetization/hive/aux_jars/
```

