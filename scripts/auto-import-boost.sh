#!/bin/bash
if ! command -v curl &> /dev/null; then
  echo "curl could not be found. Please install curl first."
  exit 1
fi
if ! command -v jq &> /dev/null; then
  echo "jq could not be found. Please install jq first."
  exit 1
fi
if [ -v BOOST_GRAPHQL_API ]; then
  api=$BOOST_GRAPHQL_API
else
  api="http://localhost:8080/graphql/query"
fi
if [ "$#" -lt 2 ]; then
  echo "Argument not supplied. Example: $0 <car_dir_path> <verified_client_address>"
  exit 1
fi
car_dir=$1
address=$2
total_deals_examined=0
total_deals_imported=0
total_imports_failed=0

echo Using Boost GraphQL API: "$api"

response=$(curl -s -X POST -H "Content-Type: application/json" -d '{"query":"query { deals(offset: 0, limit: 10000, query: \"'"$address"'\") { deals { ID ProposalLabel ClientAddress IsOffline Checkpoint DealDataRoot InboundFilePath } } }"}' "$api")

if [ ! $? -eq 0 ]
then
  echo "could not query Boost GraphQL API -- set e.g. BOOST_GRAPHQL_API=http://localhost:8080/graphql/query"
  exit 1
fi

for row in $(jq -cr '.data.deals.deals[] | .ID + "|" + .ProposalLabel + "|" + .ClientAddress + "|" + (.IsOffline|tostring) + "|" + .Checkpoint + "|" + .DealDataRoot + "|" + .InboundFilePath' <<< "$response")
do
  IFS="|" read -r id proposal client isoffline state deal_data_root inbound_file_path <<< $row
  ((total_deals_examined=total_deals_examined+1))

  if [ "$client" != "$address" ]; then
    continue
  fi
  if [ "$isoffline" != "true" ]; then
    continue
  fi
  if [ "$state" != "Accepted" ]; then
    continue
  fi
  if [ "$inbound_file_path" != "" ]; then
    continue
  fi

  file=$(realpath ${car_dir}/${deal_data_root}.car)
  if [ -f "$file" ]; then
    echo "[Importing] boostd import-data $id $file"
    boostd import-data $id $file
    if [ $? -eq 0 ]
    then
      echo "..Ok"
      ((total_deals_imported=total_deals_imported+1))
    else
      echo "..FAILED"
      ((total_imports_failed=total_imports_failed+1))
    fi
  else
    echo "[Error] $file does not exist - change the script to download from source"
    # wget http://example.com/${data_cid}.car
    # echo "[Importing] boostd import-data $id $file"
    # boostd import-data $id $file
  fi
done

echo "Imported $total_deals_imported deals ($total_imports_failed failed, $total_deals_examined examined)"
