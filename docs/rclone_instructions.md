# `rclone` Bulk Transfer Instructions

[`rclone`](https://rclone.org/) syncs files across cloud object stores, HTTP/FTP mirrors, and local filesystems. This guide covers installing and configuring `rclone` in the KBase Lakehouse environment and syncing public dataset directories (using WWPD/PDB over HTTPS as an example) into Lakehouse S3 storage prior to running ingestion pipelines in [`pyproject.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/pyproject.toml).

---

## Download and Install

Open a terminal in your Lakehouse session, download the Linux binary archive, and place `rclone` on your `PATH` (for example, `~/bin`):

```sh
curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip
bsdtar -xf rclone-current-linux-amd64.zip
mkdir -p ~/bin
cp rclone-v*-linux-amd64/rclone ~/bin/
chmod 755 ~/bin/rclone
export PATH="$HOME/bin:$PATH"
```

Add `export PATH="$HOME/bin:$PATH"` to `~/.custom_profile` so `rclone` remains on `PATH` in new shell sessions.

---

## Configure Remotes

Start interactive remote configuration with:

```sh
rclone config
```

See the [rclone provider overview](https://rclone.org/overview/) for provider-specific options. The examples below configure two remotes: `lakehouse` (MinIO-compatible S3 destination) and `pdb` (HTTPS source mirror).

### Lakehouse S3 Remote (`lakehouse`)

From the `rclone config` prompt, select `n` (New remote) and supply your Lakehouse S3 endpoint and credentials (replace `<access-key-id>` and `<secret-access-key>` with your credentials, or choose `env_auth = 2` if `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are already exported in your environment):

```text
e/n/d/r/c/s/q> n
name> lakehouse
Storage> 4          # Amazon S3 Compliant Storage Providers
provider> 30        # Minio Object Storage
env_auth> 1         # Enter AWS credentials in the next step (or 2 to read env vars)
access_key_id> <access-key-id>
secret_access_key> <secret-access-key>
region>             # Leave blank
endpoint> https://minio.berdl.kbase.us
location_constraint>
acl> 1              # 1 (private) or 2 (public-read), depending on target bucket policy
server_side_encryption>
sse_kms_key_id>
bucket_object_lock_enabled>
Edit advanced config? (y/n)> n
Keep this "lakehouse" remote? (y/e/d)> y
```

### PDB HTTPS Remote (`pdb`)

From the `rclone config` menu, select `n` (New remote) to configure an HTTPS remote pointing at `https://ftp.ebi.ac.uk`:

```text
e/n/d/r/c/s/q> n
name> pdb
Storage> 26         # HTTP
url> https://ftp.ebi.ac.uk
no_escape>
Edit advanced config? (y/n)> n
Keep this "pdb" remote? (y/e/d)> y
e/n/d/r/c/s/q> q
```

---

## Run and Verify a Sync

Preview planned transfers first with `--dry-run` (replacing `<username>` with your Lakehouse user prefix):

```sh
rclone sync --dry-run \
  pdb:pub/databases/wwpdb/pdb/data/entries/06/ \
  lakehouse:cdm-lake/users-general-warehouse/<username>/pdb/raw_data/06/
```

Once the dry-run output matches the expected files and total size, run the transfer without `--dry-run`:

```sh
rclone sync \
  pdb:pub/databases/wwpdb/pdb/data/entries/06/ \
  lakehouse:cdm-lake/users-general-warehouse/<username>/pdb/raw_data/06/
```

Verify the synced objects with `rclone ls` (or [`scripts/s3_local.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/scripts/s3_local.py) when testing against a local S3 endpoint):

```sh
rclone ls lakehouse:cdm-lake/users-general-warehouse/<username>/pdb/raw_data/06/
```
