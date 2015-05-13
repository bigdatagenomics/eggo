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

import os
from xml.etree.ElementTree import parse, fromstring


s1 = """<property>
            <name>fs.s3n.impl</name>
            <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
        </property> """
s2 = """<property>
            <name>fs.s3n.awsAccessKeyId</name>
            <value>{AWS_ACCESS_KEY_ID}</value>
        </property> """.format(**os.environ)
s3 = """<property>
            <name>fs.s3n.awsSecretAccessKey</name>
            <value>{AWS_SECRET_ACCESS_KEY}</value>
        </property> """.format(**os.environ)
p1 = fromstring(s1)
p2 = fromstring(s2)
p3 = fromstring(s3)


hadoop_home = os.environ['HADOOP_HOME']
core_site_xml = os.path.join(hadoop_home, 'etc/hadoop/core-site.xml')
tree = parse(core_site_xml)
root = tree.getroot()
root.append(p1)
root.append(p2)
root.append(p3)
tree.write(core_site_xml)
