#!/bin/bash
if ! command -v curl &> /dev/null; then
  echo "curl could not be found. Please install curl first."
  exit 1
fi
if ! command -v jq &> /dev/null; then
  echo "jq could not be found. Please install jq first."
  exit 1
fi
if [ -v MARKETS_API_INFO ]; then
  api=$MARKETS_API_INFO
else
  if [ -v MINER_API_INFO ]; then
    api=$MINER_API_INFO
  else
    echo "Environment variable MARKETS_API_INFO or MINER_API_INFO not set"
    exit 1
  fi
fi
if [ "$#" -lt 2 ]; then
  echo "Argument not supplied. Example: ./auto-import.sh <car_dir_path> <verified_client_address>"
  exit 1
fi
car_dir=$1
address=$2
token=${api%%:*}
ip=${api#*/ip4/}
ip=${ip%%/*}
port=${api%/http}
port=${port##*/}
echo Using IP: $ip, Port: $port
for row in $(curl -X POST -H "Authorization: Bearer $token" --data '{"id":1,"jsonrpc":"2.0","method":"Filecoin.MarketListIncompleteDeals","params":[]}' "http://$ip:$port/rpc/v0" | jq -cr '.result[] | .ProposalCid["/"] + "|" + .Ref.Root["/"] + "|" + (.State|tostring) + "|" + .Proposal.Client')
do
  IFS="|" read -r proposal data_cid state client <<< $row
  if [ "$client" != "$address" ]; then
    continue
  fi
  if [ "$state" != "18" ]; then
    continue
  fi
  file=$(realpath ${car_dir}/${data_cid}.car)
  if [ -f "$file" ]; then
    echo "[Importing] lotus-miner storage-deals import-data $proposal $file"
    lotus-miner storage-deals import-data $proposal $file
  else
    echo "[Error] $file does not exist - change the script to download from source"
    # wget http://example.com/${data_cid}.car
    # echo "[Importing] lotus-miner storage-deals import-data $proposal $file"
    # lotus-miner storage-deals import-data $proposal $file
  fi
done
