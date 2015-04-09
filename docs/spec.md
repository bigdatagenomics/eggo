# Eggo Design/Specification

Author(s):
Uri Laserson, Tom White

Eggo is several things:

* a repository of commonly-used public genomics data sets that are pre-
  converted into Parquet data using the BDG (and possibly GA4GH) schemas.
* a user-oriented Python API to make it easy to work with public genomics data
  sets in a Hadoop environment
* an administrator-oriented API for generating/managing the Parquet cloud data
  repository

Eggo is primarily a Python package leveraging Fabric to work with provisioned
clusters and Luigi to define ETL tasks.

Eggo wants to help enable functionality like that provided in PLINK/SEQ,
SolveBio, etc. by alleviating the need for each project to re-process the same
data sets in possibly incompatible ways.

Most users will interact with Eggo simply by reading the datasets that are
hosted in its repository. However, we’d also like to foster dataset curation by
the community. Admins would issue a pull request to the Eggo GitHub repository
with relevant changes to add a new dataset, then an Eggo maintainer would
review and commit the change before running an update to add the new dataset to
the central Eggo repository. Advanced admins would also be able to maintain
their own Eggo repositories of datasets, although in general we’d prefer to add
everything to the central Eggo repository.


## Target data sets for ingestion

* BAM files
  * NA12878 pedigree
* VCF files
  * 1000 Genomes
  * ExAC
  * TCGA MAF
* Features/Annotations
  * ENCODE
* Other
  * dbSNP
  * COSMIC


## Data set ingestion

### Eggo S3 directory structure

    s3://bdg-eggo/<dataset-name>/<format>/<edition>

`<dataset-name>` must be globally unique in the registry.  Different
versions/releases of the data sets should be considered to be different data
sets in eggo (i.e., eggo doesn’t explicitly support releases/versions of the
biological data sets; this may change in the future).

`<format>` will in principle support multiple formats/schemas.  At the moment,
we are only supporting the BDG schema, but we would like to support GA4GH as
well, and will track adding GA4GH support in ADAM.

`<edition>` refers to the specific instance of the ETL’d data, according to the
particular options that are set.  The registry JSON object will include a list
of “editions” that should be generated, specifying whether the data is
partitioned or flattened.  The possible values are:

* `basic`: no additional processing

* `flat`: the data is run through the ADAM flattener

* `locuspart`: the data is locus-partitioned (in the style of Hive partitions)

* `flat-locuspart`: both flattened and locus-partitioned

Some example S3 keys are:

    1kg-genotypes/bdg/basic
    1kg-genotypes/bdg/flat
    1kg-genotypes/bdg/flat-locuspart
    1kg-genotypes/ga4gh/basic

Temporary data during processing may be placed in the following location:

    s3://bdg-eggo/tmp

An S3 bucket for testing purposes/experimentation is

    s3://bdg-eggo-test


### Data partitioning

Currently, the data partitioning scheme that is planned to be supported is by
genome locus.  The specific details of how that will be done are TBD, however
it will likely be by chromosome and position (modulo 1e6 or 1e7, TBD). For
example, records for positions in the range `[4e6, 5e6)` would live in
the `/chr=1/pos=4` partition.

For datasets with many samples, the data may be further partitioned by sample
(hash of ID, or by date). However, this is not expected for any of the public
datasets at this point.


### Data flattening

Not all execution engines (e.g., Impala) can read/process nested data.
Therefore, eggo also offers versions of all data sets that are flattened with
the CLI `flatten` command.  Eggo also makes use of the Cloudera Kite project to
easily register the data sets with the Hive Metastore, for access through
Impala, Hive, SparkSQL, and other similar tools (TODO: @tomwhite).


## Eggo "user" API

### Configuration

Todo.

### Cluster Management

See admin API.

### Data set exploration

Todo.

Include Impala Python integration and Impala DDL.  Consider using Kite for Hive
Metastore registration

* `eggo list`

    Describe all available data sets

* `eggo info DATASET`

    Return metadata on DATASET

* `eggo get DATASET`

    Copy DATASET from S3 to "local" Hadoop cluster

* `eggo register DATASET`

    Register DATASET with the Hive metastore


## Eggo "admin/developer" API

Unfinished.

There should be a bucket (`s3://bdg-eggo-test`) that supports regular
integration tests from a test registry that includees small data sets.  There
should also be a hook to specify experimental runs that work off a specific
github fork/branch.

We should implement dry run support so the user can see all the things that
will happen.  May be difficult because it's not clear whether Fabric or Luigi
support dry run options.


### `registry/`

The `registry/` directory contains the metadata for the data sets we ingest and
convert to ADAM/Parquet.  Each data set is stored as a JSON file loosely based
on the Data Protocols spec.

### Environment variables

If using AWS, eggo will assume the following variables are set **locally**:

```bash
export SPARK_HOME= # local path to Spark
export EC2_KEY_PAIR= # EC2 name of the registered key pair
export EC2_PRIVATE_KEY_FILE= # local path to associated private key (.pem file)
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
```

Eggo will assume the following variables are set **remotely**

```bash
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
export SPARK_HOME= # remote path to Spark
export ADAM_HOME= # remote path to ADAM
export SPARK_MASTER= # Spark master host name
```

which can be done by executing

```bash
source path/to/eggo/eggo-ec2-variables.sh
```

on the remote machines.

### Desired API

**Cluster utilities**

* `eggo provision -s 3`

    Provision a cluster with 3 worker nodes on EC2.

* `eggo setup_master`/`eggo setup_slaves`

    Configure the master (driver) or slave (worker) nodes on EC2 with necessary
    prerequisites, including ADAM.

* `eggo login`

    Login to provisioned EC2 cluster.

* `eggo teardown`

    Destroy the provisioned EC2 cluster.


**Data conversion**

* `eggo toast DATASET`

    Run full pipeline on the specified dataset. There must exist a file called
    `registry/DATASET.json`.

## Examples of supported queries

Todo.

1000 genomes tutorials. It would be instructive to convert a 1000 genomes
tutorial to a BDG equivalent (such as this one [questions][questions], [answers][answers]). This
exercise would help flush out issues in the BDG datasets and tools, as well as
demonstrating a performance boost.

[questions]: ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20120229_tutorial_docs/G1K_commandline_based_tutorial_exercises_20120217.pdf
[answers]: ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20120229_tutorial_docs/G1K_commandline_based_tutorial_answers_20120217.txt
