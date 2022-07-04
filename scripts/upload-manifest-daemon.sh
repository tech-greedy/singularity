#!/bin/bash
if ! command -v curl &> /dev/null; then
  echo "curl could not be found. Please install curl first."
  exit 1
fi
if ! command -v zstd &> /dev/null; then
  echo "zstd could not be found. Please install zstd first."
  exit 1
fi
if ! command -v jq &> /dev/null; then
  echo "jq could not be found. Please install jq first."
  exit 1
fi
if [ -z "${WEB3_STORAGE_TOKEN}" ]; then
  echo "Environment variable WEB3_STORAGE_TOKEN not set"
  exit 1
fi
if [ "$#" -lt 1 ]; then
  echo "Argument not supplied. Example: ./upload-manifest-daemon.sh <datasetName> [data_preparation_service_endpoint]"
  echo "By default, data_preparation_service_endpoint is set to http://127.0.0.1:7001 if you haven't modified default.toml in singularity repo"
  exit 1
fi
endpoint=${2:-http://127.0.0.1:7001}
name=$1
for row in $(curl ${endpoint}/preparation/${name} 2>/dev/null | jq -c .generationRequests[])
do
  id=$(jq '.id' <<< "$row" | tr -d '"')
  pieceCid=$(jq '.pieceCid' <<< "$row" | tr -d '"')
  index=$(jq '.index' <<< "$row" | tr -d '"')
  status=$(jq '.status' <<< "$row" | tr -d '"')
  if [ "$status" != "completed" ]; then
    echo "Skipped [${index}] as the status is ${status}."
  fi
  echo "Working on [${index}] - generation id: ${id}"
  curl ${endpoint}/generation-manifest/${id} 2>/dev/null | zstd -f | curl -X POST -H "Authorization: Bearer ${WEB3_STORAGE_TOKEN}" -H "X-NAME: ${pieceCid}.json.zst" --data-binary @- https://api.web3.storage/upload
  echo
done
