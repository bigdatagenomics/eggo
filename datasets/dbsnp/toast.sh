#! /usr/bin/env python
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
eggo dataset dnload_datapackage \
    --datapackage datapackage.py \
    --destination hdfs:///user/ec2-user/dbsnp/raw


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
impala-shell -q "CREATE EXTERNAL TABLE prepartition LIKE PARQUET 'hdfs:///user/ec2-user/dbsnp/adam_flat_variants' STORED AS PARQUET LOCATION 'hdfs:///user/ec2-user/dbsnp/adam_flat_variants'"
hive -e "CREATE EXTERNAL TABLE postpartition PARTITIONED BY (contig STRING, pos BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/ec2-user/dbsnp/adam_flat_variants_locuspart' AS SELECT contig, pos, * FROM prepartition"





# QUINCE PROCESSING
vcf_to_locuspart_ga4gh_flatvariantcalls(
    'hdfs:///user/ec2-user/dbsnp/raw',
    'hdfs:///user/ec2-user/dbsnp/ga_variants')


# TRANSFER TO S3
distcp('hdfs:///user/ec2-user/dbsnp/adam_variants_locuspart',
       's3a://bdg-eggo/dbsnp_adam')


distcp('hdfs:///user/ec2-user/dbsnp/adam_flat_variants_locuspart',
       's3a://bdg-eggo/dbsnp_adam_flat')


distcp('hdfs:///user/ec2-user/dbsnp/ga_variants',
       's3a://bdg-eggo/dbsnp_ga4gh')
