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

# NECESSARY ENV VARS (typically set in Jenkins job config)
#     WORKSPACE (where eggo is checked out by jenkins)
#     EGGO_CONFIG
# If using S3 for DFS
#     AWS_ACCESS_KEY_ID
#     AWS_SECRET_ACCESS_KEY
# If using EC2 for workers
#     EC2_KEY_PAIR
#     EC2_PRIVATE_KEY_FILE

# TODO: test up-front that necessary env vars are set

cd $WORKSPACE

# 1. Install eggo and python requirements
virtualenv eggo_client_venv && source eggo_client_venv/bin/activate
pip install -U pip  # python-daemon only installs with a newer version of pip
pip install -U setuptools  # http://www.fabfile.org/faq.html#fabric-installs-but-doesn-t-run
pip install pytest
pip install ordereddict  # for Luigi on py2.6
pip install fabric luigi boto # depended on by eggo
pip install .  # install eggo


# TODO: remove this when #99 and #100 are closed
WORK_DIR=/tmp/eggo_work
mkdir -p $WORK_DIR
cd $WORK_DIR


# 2. Download Spark, bc eggo assumes the worker env has Spark on it somewhere
# TODO: this should be moved into the fabric_cli tools
SPARK_TARBALL_URL=http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.6.tgz
curl $SPARK_TARBALL_URL | tar xzf -


# 3. Download Hadoop (needed by Python tests)
HADOOP_TARBALL_URL=http://archive.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
curl $HADOOP_TARBALL_URL | tar xzf -
export HADOOP_HOME=$WORK_DIR/$(basename $HADOOP_TARBALL_URL .tar.gz)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME/share/hadoop/tools/lib/*  # hadoop-aws.jar: http://stackoverflow.com/questions/28029134
export PATH=$HADOOP_HOME/bin:$PATH  # bc python tests use Luigi hadoop CLI wrapper


# TODO: remove this when #99 and #100 are closed
cd $WORKSPACE


# 4. Remove data from a possible previous run of this script
eggo delete_all:config=$WORKSPACE/test/registry/test-genotypes.json


# 5. ETL the test data sets
eggo provision
eggo deploy_config
eggo setup_master
eggo toast:config=$WORKSPACE/test/registry/test-genotypes.json
# TODO: add alignments here
echo y | eggo teardown  # eggo teardown asks for confirmation


# 7. Test result correctness
py.test $WORKSPACE/test/jenkins/test_results.py

# TODO: eventually, load data into CDH cluster and test queries with Impala


# 8. Cleanup
deactivate
