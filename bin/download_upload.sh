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

# args: EPHEMERAL_MOUNT SOURCE_URL COMPRESSION_TYPE TMP_S3_PATH FINAL_S3_PATH
# COMPRESSION_TYPE can be NONE or GZIP

# download the file locally
source /root/eggo/eggo-ec2-variables.sh
export EGGO_TMP_DIR=$(mktemp -d --tmpdir=$1 tmp_eggo_XXXX)
pushd $EGGO_TMP_DIR
curl -L -O $2

# decompress if necessary
case $3 in
    NONE)
        ;;
    GZIP)
        gunzip *.gz
        ;;
    *)
        echo "Expected NONE or GZIP; got $3."
        popd
        rm -rf $EGGO_TMP_DIR
        exit 1
        ;;
esac

# upload to S3
aws s3 cp ./* $4
aws s3 mv $4 $5
popd
rm -rf $EGGO_TMP_DIR
