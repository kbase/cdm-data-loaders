#!/bin/bash
set -e

printf "\n*** starting ceph bucket & policy setup ***\n"

export AWS_ACCESS_KEY_ID="$S3_USER"
export AWS_SECRET_ACCESS_KEY="$S3_PWD"
export AWS_DEFAULT_REGION=us-east-1

AWS="aws --endpoint-url $S3_URL"

make_bucket() {
  if $AWS s3api head-bucket --bucket "$1" 2>/dev/null; then
    echo "bucket $1 already exists"
  else
    $AWS s3api create-bucket --bucket "$1" && echo "Bucket $1 created"
  fi
}

make_bucket "$S3_SQL_BUCKET"
make_bucket "$S3_TEST_BUCKET"
make_bucket "cdm-spark-job-logs"
$AWS s3api put-object --bucket "cdm-spark-job-logs" --key "spark-job-logs/testuser/.keep" >/dev/null

$AWS iam put-user-policy --user-name "$S3_SQL_RW_USER" --policy-name cdm-s3-access \
  --policy-document file:///s3_policies/sql-read-write-policy.json
echo "sql read-write user policy set"

cat > /tmp/polaris-vending-trust-policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/${S3_SQL_RW_USER}"]},"Action":["sts:AssumeRole"]}]}
EOF

if $AWS iam get-role --role-name "$POLARIS_VENDING_STS_ROLE_NAME" >/dev/null 2>&1; then
  echo "$POLARIS_VENDING_STS_ROLE_NAME role already exists"
else
  $AWS iam create-role --role-name "$POLARIS_VENDING_STS_ROLE_NAME" \
    --max-session-duration 43200 \
    --assume-role-policy-document file:///tmp/polaris-vending-trust-policy.json
  echo "$POLARIS_VENDING_STS_ROLE_NAME role created"
fi

$AWS iam put-role-policy --role-name "$POLARIS_VENDING_STS_ROLE_NAME" \
  --policy-name "${POLARIS_VENDING_STS_ROLE_NAME}-access" \
  --policy-document file:///s3_policies/ceph-polaris-vending-role-policy.json
echo "$POLARIS_VENDING_STS_ROLE_NAME role policy set"
