#!/bin/sh
set -e

POLARIS_HOST="${POLARIS_HOST:?}"
POLARIS_REALM="${POLARIS_REALM:?}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:?}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:?}"
POLARIS_SERVICE_PRINCIPAL="${POLARIS_SERVICE_PRINCIPAL:?}"
POLARIS_SERVICE_PRINCIPAL_ROLE="${POLARIS_SERVICE_PRINCIPAL_ROLE:?}"
POLARIS_SERVICE_CREDENTIAL_FILE="${POLARIS_SERVICE_CREDENTIAL_FILE:?}"
POLARIS_CATALOG_NAME="${POLARIS_CATALOG_NAME:-cdm}"
S3_ENDPOINT="${S3_ENDPOINT:-http://s3:9090}"
CATALOG_BASE_LOCATION="${CATALOG_BASE_LOCATION:-s3://sql/warehouse}"

MGMT="${POLARIS_HOST}/api/management/v1"
REALM_HEADER="Polaris-Realm: ${POLARIS_REALM}"
CONTENT_TYPE_HEADER="Content-Type: application/json"

echo "Getting OAuth token from Polaris..."
TOKEN_RESPONSE=$(curl -sf -X POST "${POLARIS_HOST}/api/catalog/v1/oauth/tokens" \
  -u "${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
  -H "${REALM_HEADER}" \
  -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL")

TOKEN="${TOKEN_RESPONSE#*\"access_token\":\"}"
TOKEN="${TOKEN%%\"*}"

if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "${TOKEN_RESPONSE}" ]; then
  echo "ERROR: Failed to get OAuth token. Response: ${TOKEN_RESPONSE}"
  exit 1
fi

AUTH_HEADER="Authorization: Bearer ${TOKEN}"

api() {
  method="$1"
  path="$2"
  data="$3"
  if [ -n "${data}" ]; then
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}" \
      -d "${data}"
  else
    curl -s -o /dev/null -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
      -H "${AUTH_HEADER}" \
      -H "${CONTENT_TYPE_HEADER}" \
      -H "${REALM_HEADER}"
  fi
}

api_response() {
  method="$1"
  path="$2"
  data="$3"
  tmpfile="$(mktemp)"
  RESPONSE_STATUS=$(curl -s -o "${tmpfile}" -w "%{http_code}" -X "${method}" "${MGMT}${path}" \
    -H "${AUTH_HEADER}" \
    -H "${CONTENT_TYPE_HEADER}" \
    -H "${REALM_HEADER}" \
    -d "${data}")
  RESPONSE_BODY="$(cat "${tmpfile}")"
  rm -f "${tmpfile}"
}

check_status() {
  status="$1"
  message="$2"
  allow_existing_grant_status="${3:-false}"
  case "${status}" in
    200|201|204) echo "  -> ${message}" ;;
    409) echo "  -> already exists" ;;
    500)
      if [ "${allow_existing_grant_status}" = "true" ]; then
        echo "  -> already configured"
      else
        echo "ERROR: unexpected status ${status}"
        exit 1
      fi
      ;;
    *) echo "ERROR: unexpected status ${status}"; exit 1 ;;
  esac
}

json_field() {
  printf "%s" "$1" | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p"
}

credential_is_valid() {
  credential_file="$1"
  [ -f "${credential_file}" ] || return 1
  client_id="$(cut -d: -f1 "${credential_file}")"
  client_secret="$(cut -d: -f2- "${credential_file}")"
  status=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${POLARIS_HOST}/api/catalog/v1/oauth/tokens" \
    -u "${client_id}:${client_secret}" \
    -H "${REALM_HEADER}" \
    -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL") || status="000"
  [ "${status}" = "200" ]
}

echo "Setting up Polaris principal ${POLARIS_SERVICE_PRINCIPAL}..."
if credential_is_valid "${POLARIS_SERVICE_CREDENTIAL_FILE}"; then
  echo "  -> existing credentials at ${POLARIS_SERVICE_CREDENTIAL_FILE} still valid"
else
  status=$(api POST "/principals" "{
    \"principal\": {
      \"name\": \"${POLARIS_SERVICE_PRINCIPAL}\",
      \"type\": \"USER\",
      \"properties\": {}
    }
  }")
  check_status "${status}" "principal created"

  status=$(api PUT "/principals/${POLARIS_SERVICE_PRINCIPAL}/principal-roles" "{
    \"principalRole\": {
      \"name\": \"${POLARIS_SERVICE_PRINCIPAL_ROLE}\"
    }
  }")
  check_status "${status}" "principal role assigned" true

  api_response POST "/principals/${POLARIS_SERVICE_PRINCIPAL}/reset" "{}"
  check_status "${RESPONSE_STATUS}" "principal credentials reset"

  client_id="$(json_field "${RESPONSE_BODY}" "clientId")"
  client_secret="$(json_field "${RESPONSE_BODY}" "clientSecret")"
  if [ -z "${client_id}" ] || [ -z "${client_secret}" ]; then
    echo "ERROR: Could not parse principal credentials. Response: ${RESPONSE_BODY}"
    exit 1
  fi

  mkdir -p "$(dirname "${POLARIS_SERVICE_CREDENTIAL_FILE}")"
  printf "%s:%s" "${client_id}" "${client_secret}" > "${POLARIS_SERVICE_CREDENTIAL_FILE}"
  chmod 600 "${POLARIS_SERVICE_CREDENTIAL_FILE}"
  echo "  -> principal credentials written to ${POLARIS_SERVICE_CREDENTIAL_FILE}"
fi

echo "Ensuring default Polaris catalog '${POLARIS_CATALOG_NAME}'..."
status=$(api POST "/catalogs" "{
  \"catalog\": {
    \"name\": \"${POLARIS_CATALOG_NAME}\",
    \"type\": \"INTERNAL\",
    \"properties\": {
      \"default-base-location\": \"${CATALOG_BASE_LOCATION}\"
    },
    \"storageConfigInfo\": {
      \"storageType\": \"S3\",
      \"allowedLocations\": [\"${CATALOG_BASE_LOCATION}\"],
      \"endpoint\": \"${S3_ENDPOINT}\",
      \"endpointInternal\": \"${S3_ENDPOINT}\",
      \"pathStyleAccess\": true,
      \"region\": \"us-east-1\",
      \"stsUnavailable\": true
    }
  }
}")
check_status "${status}" "catalog '${POLARIS_CATALOG_NAME}' created"

echo "Granting catalog admin role on '${POLARIS_CATALOG_NAME}' to '${POLARIS_SERVICE_PRINCIPAL_ROLE}'..."
status=$(api POST "/catalogs/${POLARIS_CATALOG_NAME}/catalog-roles" "{
  \"catalogRole\": {
    \"name\": \"cdm_admin\"
  }
}")
check_status "${status}" "catalog role created"

status=$(api PUT "/catalogs/${POLARIS_CATALOG_NAME}/catalog-roles/cdm_admin/grants" "{
  \"type\": \"catalog\",
  \"privilege\": \"CATALOG_MANAGE_CONTENT\"
}")
check_status "${status}" "CATALOG_MANAGE_CONTENT granted" true

status=$(api PUT "/principal-roles/${POLARIS_SERVICE_PRINCIPAL_ROLE}/catalog-roles/${POLARIS_CATALOG_NAME}" "{
  \"catalogRole\": {
    \"name\": \"cdm_admin\"
  }
}")
check_status "${status}" "catalog role assigned to ${POLARIS_SERVICE_PRINCIPAL_ROLE}" true

echo "Polaris lakehouse setup complete."
