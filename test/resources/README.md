### `chr22.small.vcf.gz'

```bash
curl -s ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | gunzip | head -n 100 > chr22.small.vcf
gzip chr22.small.vcf
```