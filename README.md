# eggo

Provides Parquet-formatted public 'omics datasets in S3 for easily using ADAM
and the Hadoop stack (including Spark and Impala).

### `registry/`

This is the list of raw files we ingest into ADAM format, encoded as a
collection of JSON objects with the following schema:

Field | Description
----- | -----------
`dataset` | Which main dataset does this file belong to (e.g., 1000 Genomes, ENCODE, etc)
`filetype` | The raw data's file type.  This will determine how the data in converted.
`source` | A URL for the raw data (probably FTP)
`target` | The target location in S3 to deposit the converted data
