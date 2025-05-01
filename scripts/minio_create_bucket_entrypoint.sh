#!/bin/bash

mc alias set minio http://minio:9002 minio minio123

# make deltalake bucket
if ! mc ls minio/cdm-lake 2>/dev/null; then
  mc mb minio/cdm-lake && echo 'Bucket cdm-lake created'
else
  echo 'bucket cdm-lake already exists'
fi

# create policies
mc admin policy create minio cdm-read-only-policy /config/cdm-read-only-policy.json
mc admin policy create minio cdm-read-write-policy /config/cdm-read-write-policy.json

# make read only user for user notebook
mc admin user add minio minio-readonly minio123
mc admin policy attach minio cdm-read-only-policy --user=minio-readonly
echo 'CDM Read-only user and policy set'

# make read/write user
mc admin user add minio minio-readwrite minio123
mc admin policy attach minio cdm-read-write-policy --user=minio-readwrite
echo 'CDM read-write user and policy set'