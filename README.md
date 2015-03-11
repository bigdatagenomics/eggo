# eggo

Provides Parquet-formatted public 'omics datasets in S3 for easily using ADAM
and the Hadoop stack (including Spark and Impala). Instead of acquiring and
converting the data sets yourself, simply get them from the eggo S3 bucket:

```
s3://bdg-eggo
```

## User interface

Not implemented yet.

### Getting started

```
git clone https://github.com/bigdatagenomics/eggo.git
cd eggo
python setup.py install
```

TODO: pip installable scripts for listing datasets ets.

You need to [install](http://www.fabfile.org/installing.html) `fabric` too.

## Developer/maintainer interface

The `eggo` machinery uses Fabric and Luigi for its operation.

### `registry/`

The `registry/` directory contains the metadata for the data sets we ingest and
convert to ADAM/Parquet.  Each data set is stored as a JSON file loosely based
on the Data Protocols spec.

### Environment

If using AWS, ensure the following variables are set locally:

```
export SPARK_HOME= # local path to Spark
export EC2_KEY_PAIR= # EC2 name of the registered key pair
export EC2_PRIVATE_KEY_FILE= # local path to associated private key (.pem file)
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
```

These variables must be set remotely, which can be done by `source eggo-
ec2-variables.sh`:

```
export AWS_ACCESS_KEY_ID= # AWS credentials
export AWS_SECRET_ACCESS_KEY= # AWS credentials
export SPARK_HOME= # remote path to Spark
export ADAM_HOME= # remote path to ADAM
export SPARK_MASTER= # Spark master host name
```

### Setting up a cluster

```bash
cd path/to/eggo

# provision a cluster on EC2 with 5 slave (worker) nodes
fab provision:5,r3.2xlarge

# configure proper environment on the instances
fab setup_master
fab setup_slaves

# (Cloudera infra-only)
./tag-my-instances.py

# get an interactive shell on the master node
fab login

# destroy the cluster
fab teardown
```

### Converting data sets

The `toast` command will build the Luigi DAG for downloading the necessary data
to S3 and running the ADAM command to transform it to Parquet.

```
# toast the 1000 Genomes data set
fab toast:registry/1kg.json
```


### Configuration

Environment variables that should be set


ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem -s 3 -t m3.large -z us-east-1a --delete-groups --copy-aws-credentials launch eggo
ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem login eggo
ec2/spark-ec2 -k laserson-cloudera -i ~/.ssh/laserson-cloudera.pem destroy eggo

curl http://169.254.169.254/latest/meta-data/public-hostname


def verify_env():
    require('SPARK_HOME')
    require('EC2_KEY_PAIR')
    require('EC2_PRIVATE_KEY_FILE')
    require('AWS_ACCESS_KEY_ID')
    require('AWS_SECRET_ACCESS_KEY')



TODO: have to CLI commands: `eggo` for users and `toaster` for maintainers.




### Desired API

**User-facing commands**

* `eggo list`

    Describe all available data sets

* `eggo info DATASET`

    Return metadata on DATASET


**Cluster utilities**

* `eggo provision -s 3`

    Provision a cluster with 3 worker nodes on EC2.

* `eggo setup_master`

    Configure the master (driver) node on EC2 with necessary prerequisites,
    including ADAM.

* `eggo login`

    Login to provisioned EC2 cluster.

* `eggo teardown`

    Destroy the provisioned EC2 cluster.


**Data conversion**

* `eggo toast DATASET`

    Run full pipeline on the specified dataset. There must exist a file called
    `registry/DATASET.json`.
