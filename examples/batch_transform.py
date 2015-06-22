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

"""Luigi workflow to convert a batch of bams to ADAM"""
import os.path

import luigi
from luigi import Parameter, WrapperTask
from luigi.s3 import S3PathTask, S3Target, S3Client

from eggo.dag import ADAMBasicTask


def convert_s3n(s):
    return s.replace('s3://', 's3n://')


class ADAMTransformTask(ADAMBasicTask):

    source_uri = Parameter()
    destination_uri = Parameter(None)

    @property
    def adam_command(self):
        return 'transform {source} {target}'.format(
            source=self.input().path,
            target=self.output().path)

    def requires(self):
        return S3PathTask(path=self.source_uri)

    def output(self):
        if self.destination_uri is not None:
            return S3Target(self.destination_uri)
        return S3Target(self.source_uri.replace('.bam', '.adam'))


class BatchTransform(WrapperTask):

    source_folder = Parameter()
    destination_folder = Parameter(None)

    def requires(self):
        source_folder_s3n = convert_s3n(self.source_folder)

        if self.destination_folder is not None:
            destination_folder_s3n = convert_s3n(self.destination_folder)
        else:
            destination_folder_s3n = source_folder_s3n

        s3 = S3Client()
        for key_path in s3.list(self.source_folder):
            if key_path.endswith('.bam') or key_path.endswith('.sam'):
                source_uri = os.path.join(source_folder_s3n, key_path)
                destination_uri = os.path.join(destination_folder_s3n, key_path.replace('.bam', '.adam'))
                yield ADAMTransformTask(source_uri=source_uri, destination_uri=destination_uri)


if __name__ == '__main__':
    luigi.run()
