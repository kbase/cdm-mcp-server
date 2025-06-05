#!/bin/bash

mc alias set minio http://minio:9002 minio minio123

# make deltalake bucket
if ! mc ls minio/cdm-lake 2>/dev/null; then
  mc mb minio/cdm-lake && echo 'Bucket cdm-lake created'
else
  echo 'bucket cdm-lake already exists'
fi

# make spark job logs bucket
if ! mc ls minio/cdm-spark-job-logs 2>/dev/null; then
  mc mb minio/cdm-spark-job-logs && echo 'Bucket cdm-spark-job-logs created'
else
  echo 'bucket cdm-spark-job-logs already exists'
fi

# create spark-job-logs directory inside the bucket
# Create a dummy file to ensure the directory exists, then remove it
echo "dummy" | mc pipe minio/cdm-spark-job-logs/spark-job-logs/.dummy
echo 'spark-job-logs directory created in cdm-spark-job-logs bucket'


# create policies
mc admin policy create minio cdm-read-only-policy /config/cdm-read-only-policy.json
mc admin policy create minio cdm-read-write-policy /config/cdm-read-write-policy.json
mc admin policy create minio cdm-spark-job-logs-policy /config/cdm-spark-job-logs-policy.json

# make read only user for user notebook
mc admin user add minio minio-readonly minio123
mc admin policy attach minio cdm-read-only-policy --user=minio-readonly
mc admin policy attach minio cdm-spark-job-logs-policy --user=minio-readonly
echo 'CDM Read-only user and policy set'

# make read/write user
mc admin user add minio minio-readwrite minio123
mc admin policy attach minio cdm-read-write-policy --user=minio-readwrite
mc admin policy attach minio cdm-spark-job-logs-policy --user=minio-readwrite
echo 'CDM read-write user and policy set'

# make spark events log access user
mc admin user add minio minio-log-access minio123
mc admin policy attach minio cdm-spark-job-logs-policy --user=minio-log-access
echo 'Spark events log access user and policy set'