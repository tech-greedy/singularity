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
if [ "$#" -lt 2 ]; then
  echo "Argument not supplied. Example: ./upload-manifest-daemon.sh <datasetName> <slugName> [data_preparation_service_endpoint]"
  echo "<datasetName> is the dataset name shown in 'singularity prep list'"
  echo "<slugName> is the slug name displayed on Slingshot V3 website"
  echo "[data_preparation_service_endpoint] is optional and by default set to http://127.0.0.1:7001"
  exit 1
fi
endpoint=${3:-http://127.0.0.1:7001}
name=$1
slug=$2
if curl ${endpoint}/preparation/${name} 2>/dev/null | jq -c '.scanningStatus, .generationRequests[].status' | grep -v completed; then
  echo "There are still generations not completed. Please check 'singularity prep list'"
  exit 1
fi
for row in $(curl ${endpoint}/preparation/${name} 2>/dev/null | jq -c '.generationRequests[]|[.id, .pieceCid, .index]')
do
  id=$(jq '.[0]' <<< "$row" | tr -d '"')
  pieceCid=$(jq '.[1]' <<< "$row" | tr -d '"')
  index=$(jq '.[2]' <<< "$row" | tr -d '"')
  echo "Working on [${index}] - generation id: ${id}, pieceCid: ${pieceCid}"
  curl ${endpoint}/generation-manifest/${id} 2>/dev/null | jq ".dataset = \"${slug}\""| zstd -f | curl -X POST -H "Authorization: Bearer ${WEB3_STORAGE_TOKEN}" -H "X-NAME: ${pieceCid}.json.zst" --data-binary @- https://api.web3.storage/upload
  echo
done
