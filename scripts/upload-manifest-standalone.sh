#!/bin/bash
if ! command -v curl &> /dev/null; then
  echo "curl could not be found. Please install curl first."
  exit 1
fi
if ! command -v zstd &> /dev/null; then
  echo "zstd could not be found. Please install zstd first."
  exit 1
fi
if [ -z "${WEB3_STORAGE_TOKEN}" ]; then
  echo "Environment variable WEB3_STORAGE_TOKEN not set"
  exit 1
fi
if [ "$#" -ne 1 ]; then
  echo "Argument not supplied. Example: ./upload-manifest-daemon.sh <manifest_folder>"
  echo "The <manifest_folder> is the folder where contains manifest json files."
  exit 1
fi
folder=$1
for path in $(find ${folder} -name '*.json')
do
  basename=${path##*/}
  echo "Working on ${basename}"
  cat $path | zstd -f | curl -X POST -H "Authorization: Bearer ${WEB3_STORAGE_TOKEN}" -H "X-NAME: ${basename}.zst" --data-binary @- https://api.web3.storage/upload
  echo
done
