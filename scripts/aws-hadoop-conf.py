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

"""Generate necessary S3 properties for Hadoop client config.

Requires these environment variables to be set:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
"""

from __future__ import print_function

import os
from xml.etree.ElementTree import parse, fromstring, tostring

from click import group, option
from cm_api.api_client import ApiResource


def generate_xml_elements():
    s3_1 = ('<property>\n'
            '    <name>fs.s3.impl</name>\n'
            '    <value>org.apache.hadoop.fs.s3.S3FileSystem</value>\n'
            '</property>')
    s3_2 = ('<property>\n'
            '    <name>fs.s3.awsAccessKeyId</name>\n'
            '    <value>{AWS_ACCESS_KEY_ID}</value>\n'
            '</property>').format(**os.environ)
    s3_3 = ('<property>\n'
            '    <name>fs.s3.awsSecretAccessKey</name>\n'
            '    <value>{AWS_SECRET_ACCESS_KEY}</value>\n'
            '</property>').format(**os.environ)
    s3n_1 = ('<property>\n'
             '    <name>fs.s3n.impl</name>\n'
             '    <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>\n'
             '</property>')
    s3n_2 = ('<property>\n'
             '    <name>fs.s3n.awsAccessKeyId</name>\n'
             '    <value>{AWS_ACCESS_KEY_ID}</value>\n'
             '</property>').format(**os.environ)
    s3n_3 = ('<property>\n'
             '    <name>fs.s3n.awsSecretAccessKey</name>\n'
             '    <value>{AWS_SECRET_ACCESS_KEY}</value>\n'
             '</property>').format(**os.environ)
    s3a_1 = ('<property>\n'
             '    <name>fs.s3a.impl</name>\n'
             '    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>\n'
             '</property>')
    s3a_2 = ('<property>\n'
             '    <name>fs.s3a.awsAccessKeyId</name>\n'
             '    <value>{AWS_ACCESS_KEY_ID}</value>\n'
             '</property>').format(**os.environ)
    s3a_3 = ('<property>\n'
             '    <name>fs.s3a.awsSecretAccessKey</name>\n'
             '    <value>{AWS_SECRET_ACCESS_KEY}</value>\n'
             '</property>').format(**os.environ)

    s = [s3_1, s3_2, s3_3, s3n_1, s3n_2, s3n_3, s3a_1, s3a_2, s3a_3]
    p = [fromstring(x) for x in s]
    return p


def get_s3_properties():
    elts = generate_xml_elements()
    return [e.find('name').text for e in elts]


@group(context_settings={'help_option_names': ['-h', '--help']})
def main():
    """Utility to generate S3 properties for Hadoop client config"""
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        raise ValueError("AWS_ACCESS_KEY_ID not set")
    if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise ValueError("AWS_SECRET_ACCESS_KEY not set")


@main.command()
def dump():
    """Dump XML to stdout (note: this will print AWS credentials)"""
    elts = generate_xml_elements()
    for e in elts:
        print(tostring(e))


@main.command()
@option('-p', '--path',
        help='The path to the XML file; typically '
             '$HADOOP_HOME/etc/hadoop/core-site.xml')
def update_xml(path):
    """Update config by modifying XML config file"""
    elts = generate_xml_elements()
    tree = parse(path)
    root = tree.getroot()
    for e in elts:
        root.append(e)
    tree.write(path)


@main.command()
@option('--cm-host', help='Hostname for Cloudera Manager')
@option('--cm-port', default=7180, show_default=True,
        help='Port for Cloudera Manager')
@option('--username', default='admin', show_default=True, help='CM username')
@option('--password', default='admin', show_default=True, help='CM password')
def update_cm(cm_host, cm_port, username, password):
    """Update config using the CM API (note: will restart service)"""
    elts = generate_xml_elements()
    cm_api = ApiResource(cm_host, username=username, password=password,
                         server_port=cm_port, version=9)
    cluster = list(cm_api.get_all_clusters())[0]
    hdfs = filter(lambda x: x.type == 'HDFS',
                  list(cluster.get_all_services()))[0]
    print("Updating HFDS core-site.xml safety valve...")
    _ = hdfs.update_config({
        'core_site_safety_valve': '\n'.join(tostring(e) for e in elts)})
    print("Deploying client config across the cluster...")
    cluster.deploy_client_config().wait()
    print("Restarting necessary services...")
    cluster.restart().wait()
    print("Done!")


@main.command()
@option('--cm-host', help='Hostname for Cloudera Manager')
@option('--cm-port', default=7180, show_default=True,
        help='Port for Cloudera Manager')
@option('--username', default='admin', show_default=True, help='CM username')
@option('--password', default='admin', show_default=True, help='CM password')
def reset_cm(cm_host, cm_port, username, password):
    """Elim S3 config from CM API safety valve (service restart necessary)"""
    s3_props = set(get_s3_properties())
    cm_api = ApiResource(cm_host, username=username, password=password,
                         server_port=cm_port, version=9)
    cluster = list(cm_api.get_all_clusters())[0]
    hdfs = filter(lambda x: x.type == 'HDFS',
                  list(cluster.get_all_services()))[0]
    print("Getting current safety valve config")
    current_config = hdfs.get_config('full')[0]['core_site_safety_valve'].value
    # need the "<foo>...</foo>" to make it valid XML (bc it requires root elt)
    elts = list(fromstring('<foo>' + current_config + '</foo>'))
    new_elts = filter(lambda x: x.find('name').text not in s3_props, elts)
    print("Updating safety valve and deleting S3 config")
    _ = hdfs.update_config({
        'core_site_safety_valve': '\n'.join(tostring(e) for e in new_elts)})
    print("Deploying client config across the cluster...")
    cluster.deploy_client_config().wait()
    print("Restarting necessary services...")
    cluster.restart().wait()
    print("Done!")

if __name__ == '__main__':
    main()
