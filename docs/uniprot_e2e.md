# UniProt End to End Import Pipeline


## rclone config

```yaml
[lakehouse]
type = s3
provider = Minio
env_auth = true
region = us-east-1
endpoint = https://minio.berdl.kbase.us
acl = public-read
bucket_acl = public-read

[uniprot]
- type: http
- url: https://ftp.ebi.ac.uk/pub/databases/uniprot/current_release/
```

## Download raw files from UniProt

```sh
rclone copy uniprot: lakehouse:cdm-lake/tenant-general-warehouse/refdata/datasets/uniprot/raw_data/{release_id}/ --no-traverse --files-from scripts/uniprot_files.txt --s3-no-check-bucket -M -v
```


## Copy files over to CTS bucket for XML processing
