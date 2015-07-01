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

"""Luigi workflow to convert bams to ADAM and count kmers"""

import luigi
from luigi import Parameter
from luigi.s3 import S3Target, S3PathTask

from eggo.dag import ADAMBasicTask


class ADAMTransformTask(ADAMBasicTask):

    source_uri = Parameter()

    @property
    def adam_command(self):
        return 'transform {source} {target}'.format(
            source=self.input().path,
            target=self.output().path)

    def requires(self):
        return S3PathTask(path=self.source_uri)

    def output(self):
        return S3Target(self.source_uri.replace('.bam', '.adam'))


class CountKmersTask(ADAMBasicTask):

    source_uri = Parameter()
    kmer_length = Parameter(21)

    @property
    def adam_command(self):
        return 'count_kmers {source} {target} {kmer_length}'.format(
            source=self.input().path,
            target=self.output().path,
            kmer_length=self.kmer_length)

    def requires(self):
        return ADAMTransformTask(source_uri=self.source_uri)

    def output(self):
        return S3Target(self.source_uri.replace('.bam', '.kmer'))


if __name__ == '__main__':
    luigi.run()
