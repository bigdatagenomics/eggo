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

"""Add necessary S3N properties to Hadoop config for client to work with S3.

Requires these environment variables to be set:
    HADOOP_HOME
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
"""

import os
from xml.etree.ElementTree import parse, fromstring


s3_1 = """<property>
              <name>fs.s3.impl</name>
              <value>org.apache.hadoop.fs.s3.S3FileSystem</value>
          </property> """
s3_2 = """<property>
              <name>fs.s3.awsAccessKeyId</name>
              <value>{AWS_ACCESS_KEY_ID}</value>
          </property> """.format(**os.environ)
s3_3 = """<property>
              <name>fs.s3.awsSecretAccessKey</name>
              <value>{AWS_SECRET_ACCESS_KEY}</value>
          </property> """.format(**os.environ)
s3n_1 = """<property>
               <name>fs.s3n.impl</name>
               <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
           </property> """
s3n_2 = """<property>
               <name>fs.s3n.awsAccessKeyId</name>
               <value>{AWS_ACCESS_KEY_ID}</value>
           </property> """.format(**os.environ)
s3n_3 = """<property>
               <name>fs.s3n.awsSecretAccessKey</name>
               <value>{AWS_SECRET_ACCESS_KEY}</value>
           </property> """.format(**os.environ)
s3a_1 = """<property>
               <name>fs.s3a.impl</name>
               <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
           </property> """
s3a_2 = """<property>
               <name>fs.s3a.awsAccessKeyId</name>
               <value>{AWS_ACCESS_KEY_ID}</value>
           </property> """.format(**os.environ)
s3a_3 = """<property>
               <name>fs.s3a.awsSecretAccessKey</name>
               <value>{AWS_SECRET_ACCESS_KEY}</value>
           </property> """.format(**os.environ)

s = [s3_1, s3_2, s3_3, s3n_1, s3n_2, s3n_3, s3a_1, s3a_2, s3a_3]
p = [fromstring(x) for x in s]

hadoop_home = os.environ['HADOOP_HOME']
core_site_xml = os.path.join(hadoop_home, 'etc/hadoop/core-site.xml')
tree = parse(core_site_xml)
root = tree.getroot()
for x in p:
    root.append(x)
tree.write(core_site_xml)
