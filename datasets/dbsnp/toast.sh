#! /usr/bin/env bash
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# DOWNLOAD VCF FILES
eggo-data dnload_raw \
    --input datapackage.json \
    --output hdfs:///user/ec2-user/dbsnp/raw


# ADAM PROCESSING
# convert to ADAM format
~/adam/bin/adam-submit --master yarn-client --driver-memory 8g \
    --num-executors $TOTAL_EXECUTORS --executor-cores $CORES_PER_EXECUTOR \
    --executor-memory $MEMORY_PER_EXECUTOR \
    -- \
    vcf2adam -onlyvariants \
    hdfs:///user/ec2-user/dbsnp/raw \
    hdfs:///user/ec2-user/dbsnp/adam_variants

# flatten parquet data
~/adam/bin/adam-submit --master yarn-client --driver-memory 8g \
    --num-executors $TOTAL_EXECUTORS --executor-cores $CORES_PER_EXECUTOR \
    --executor-memory $MEMORY_PER_EXECUTOR \
    -- \
    flatten \
    hdfs:///user/ec2-user/dbsnp/adam_variants \
    hdfs:///user/ec2-user/dbsnp/adam_flat_variants


# locus partition parquet data with Hive
# TODO: the following query is manually crafted bc CREATE TABLE LIKE PARQUET chokes on ENUM
SEGMENT_SIZE=1000000
NUM_REDUCERS=300
TABLE_SCHEMA='`variantErrorProbability` INT, `contig__contigName` STRING, `contig__contigLength` BIGINT, `contig__contigMD5` STRING, `contig__referenceURL` STRING, `contig__assembly` STRING, `contig__species` STRING, `contig__referenceIndex` INT, `start` BIGINT, `end` BIGINT, `referenceAllele` STRING, `alternateAllele` STRING, `svAllele__type` BINARY, `svAllele__assembly` STRING, `svAllele__precise` BOOLEAN, `svAllele__startWindow` INT, `svAllele__endWindow` INT, `isSomatic` BOOLEAN'
hive -e "CREATE EXTERNAL TABLE prepartition ($TABLE_SCHEMA) STORED AS PARQUET LOCATION 'hdfs:///user/ec2-user/dbsnp/adam_flat_variants'"
hive -e "CREATE EXTERNAL TABLE postpartition ($TABLE_SCHEMA) PARTITIONED BY (chr STRING, pos BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/ec2-user/dbsnp/adam_flat_variants_locuspart'"
export HIVE_OPTS="--hiveconf mapreduce.job.reduces=$NUM_REDUCERS --hiveconf mapreduce.map.memory.mb=8192 --hiveconf mapreduce.reduce.memory.mb=8192 --hiveconf mapreduce.reduce.java.opts=-Xmx8192m --hiveconf hive.exec.dynamic.partition.mode=nonstrict --hiveconf hive.exec.max.dynamic.partitions=3000"
hive  -e "INSERT OVERWRITE TABLE postpartition PARTITION (chr, pos) SELECT *, contig__contigName, floor(start / $SEGMENT_SIZE) * $SEGMENT_SIZE FROM prepartition DISTRIBUTE BY contig__contigName, floor(start / $SEGMENT_SIZE) * $SEGMENT_SIZE"
hive -e "DROP TABLE prepartition"
hive -e "DROP TABLE postpartition"


# TRANSFER TO S3
hadoop distcp \
    hdfs:///user/ec2-user/dbsnp/adam_flat_variants_locuspart \
    s3n://bdg-eggo/dbsnp_flat
