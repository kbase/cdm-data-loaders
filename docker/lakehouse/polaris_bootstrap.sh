#!/bin/sh

POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_BOOTSTRAP_CREDENTIAL="${POLARIS_BOOTSTRAP_CREDENTIAL:-POLARIS,root,s3cr3t}"

java -jar /deployments/polaris-admin-tool.jar bootstrap \
  "--realm=${POLARIS_REALM}" \
  "--credential=${POLARIS_BOOTSTRAP_CREDENTIAL}"

status=$?
if [ "${status}" -eq 0 ] || [ "${status}" -eq 3 ]; then
  exit 0
fi
exit "${status}"
