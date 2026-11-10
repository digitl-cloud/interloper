# interloper-aws

AWS integration for interloper: an `S3Destination` that stores asset data in
an Amazon S3 bucket, and the `AWSConnection` that holds the credentials.

`S3Destination` is an `ObjectStoreDestination`, the same base as the Cloud
Storage destination in `interloper-google-cloud`, so the two behave
identically: one object per partition in a hive-partitioned layout, Parquet
(the default), JSONL or CSV, the partition column in the path only, and each
object's row count stored as `row_count` user metadata.

```
s3://{bucket}/{prefix}/{dataset}/{table}/data.{ext}
s3://{bucket}/{prefix}/{dataset}/{table}/{column}={partition}/data.{ext}
```

## Usage

```python
import interloper as il
from interloper_aws import AWSConnection, S3Destination

destination = S3Destination(
    connection=AWSConnection(region="eu-central-1"),
    bucket="acme-lake",
    prefix="raw",
    format="parquet",
)

source = Shop(destinations=destination)
```

## Credentials

The connection takes an access key pair (`access_key_id`,
`secret_access_key`, plus `session_token` for temporary credentials) and a
`region`, defaulting to `eu-central-1`. Each field also loads from the
standard environment variables: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION`.

Leave the key pair empty and boto3's default credential chain applies:
environment, shared config files, then the instance or pod role (an EC2
instance profile, EKS IRSA or Pod Identity). In a cluster, that means no
stored secret at all: grant the workload's role access to the bucket.

The connection's check calls STS `GetCallerIdentity`, which needs no IAM
permission, so a failing check means the credentials themselves are wrong.
The bucket picker calls `ListBuckets`.

## Permissions

The destination needs, on the bucket and its objects:

- `s3:PutObject` to write
- `s3:GetObject` to read, and to count objects written without `row_count`
  metadata
- `s3:ListBucket` for partition row counts
- `s3:ListAllMyBuckets` for the bucket picker only

## Notes

`ListObjectsV2` returns no user metadata, so partition row counts issue one
`HeadObject` per listed object. That is still no download: the count comes
from the `row_count` metadata stamped at write time. Objects written by other
tools, without that metadata, are downloaded and counted.
