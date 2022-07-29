# Getting Started

## Installation
The tool works in Mac/Linux/Windows platform and is distributed using NPM.
```shell
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
source ~/.bashrc
nvm install 16
npm i -g @techgreedy/singularity
```
If you are using a root user, you may encounter some permission error during installation. If so, use below commands to work around:
```shell
chown -R $(whoami) ~/
npm config set unsafe-perm true
npm config set user 0
```

## Initialization
The tool works as a daemon service running in the background while providing the CLI to interact with the service. To initialize the service repository:
```shell
export SINGULARITY_PATH=/the/path/to/the/repo
singularity init
```
A default `config.toml` will be copied over to the repo. By default it will enable all modules with reasonable settings.

Now you may Start the daemon service by running
```shell
singularity daemon
```

## Data Preparation
### Start deal preparation
The data preparation module will scan and convert a local folder recursively into CAR files ready to be onboarded to Filecoin Network. 

You need to specify three arguments - the name of the dataset, the path of the dataset and the output directory for CAR files.
```shell
singularity prep create MyData ~/dataset/folder ~/outDir 
```
You can then check the progress of deal preparation by running
```shell
singularity prep list
singularity prep status MyData
```
To look into what each CAR file is composed of, you can run
```shell
singularity prep generation-status --dataset MyData <index>
```
You can also pause, resume or retry the scanning or generation jobs using
```shell
singularity prep pause
singularity prep resume
singularity prep retry
```

### Distribute CAR files
The easiest way to distribute CAR file to storage providers is via an HTTP server. You can set it up with `nginx`:
```shell
sudo apt install nginx
```
Edit `/etc/nginx/sites-available/default` and add below lines
```
server {
  ...
  location / {
    root /home/user/outDir;
  }
  ...
}
```

## Deal Making
### Prerequisite
You need to hand pick storage providers by yourself. A good place to find them is [Filecoin Slack channel](filecoinproject.slack.com), [ESPA Slack channel](web3espa.slack.com).

Most storage providers today are interested in taking verified deals so you need to familiar yourself with [Filecoin Plus](https://github.com/filecoin-project/filecoin-plus-client-onboarding).
You can get some small amount of datacap in [Filecoin Plus Registry](https://plus.fil.org/).

### Setup lotus lite node
Reference: [Lite node](https://lotus.filecoin.io/lotus/install/lotus-lite/)

Download lotus binary from [lotus release page](https://github.com/filecoin-project/lotus/releases)

Start lite-node using below command
```shell
FULLNODE_API_INFO="wss://api.chain.love" lotus daemon --lite
```

Import your wallet key into lotus lite node
```shell
lotus wallet import
```

Check if the wallet address has been sucessfully imported, then you should be able to make deals using your imported wallet address
```shell
lotus wallet list
```
If you are customizing the `LOTUS_PATH`, you need to make sure the singularity daemon uses the same environment variable.

### Make deals to storage providers
Since we are doing this for the first time, we'd like to only start with a few deals to test the flow. The below command sends out 10 deals from 'MyData' dataset to storage provider 'f01111'. The client address is the wallet address that you'd like to propose deals from. 
```shell
# singularity repl start -m <n> <dataset_id> <storage_providers> <client_address>
singularity repl start -m 10 MyData f01111 f1...
```
On the storage provider side, they can use [auto-import.sh](https://github.com/tech-greedy/singularity/blob/main/scripts/auto-import.sh) to import those deals automatically. Of course you'll have to tell them where to download those CAR files.

If the storage provider is using boost market node and you've already setup your HTTP server, you can add the HTTP link prefix in the below command so the boost market node will take care of the file download.
```shell
singularity repl start -m 10 -u "http://my.datset.org/car/" MyData f01111 f1...
```
Once everything works smoothly on both side, you can start making deals automatically with to a speed agreed by both parties, i.e.
```shell
singularity repl start -m 10 -c '0 * * * *' -x 1000 -xp 100 MyData f01111 f1...
```
This command creates a schedule to send 10 deals each hour for up to 1000 deals in total with up to 100 pending deals. 

## Indexing and Retrieval
Each file and folder in the dataset has its own unique CID. Usually, when you retrieve the file or folder from storage provider, you need to specify the CID of the file or folder.

However, for most people coming from web2.0, we are used to retrieving resources using URI. That's where the index and retrieval module shines.

The index module publishes the mapping from URI path to the CID so end users can retrieve content using URI instead of CID.

### Prerequisites
Install [IPFS](https://docs.ipfs.tech/install/) and start the daemon service
```shell
ipfs init
ipfs daemon
```
The index will be published on IPFS which is required to be run by client and retrievers.

### Create Index
Index creation is as simple as running a single command for a specific dataset. 
```shell
singularity index create MyData
```
It will return the IPFS path that contains the index and you can choose to publish it with DNSLink to make it more end-user friendly.
```
Add or update the TXT record for _dnslink.mydata.net
  _dnslink.mydata.net  34  IN  TXT "dnslink=/ipfs/bafy..."
```

### Retrieval
With index published on IPFS, the end user can now browse the dataset from Filecoin network
```shell
# ls command tells what file or folder is inside the sub path of the dataset
singularity-retrieve ls -v singualrity://ipns/mydata.net/sub/path
# show command tells the CID of the file or folder and how to assemble them back 
singularity-retrieve show singualrity://ipns/mydata.net/sub/path
# cp command retrieves the data from a specific storage provider to a local path and reassemble them if needed
singularity-retrieve cp -p f01111 singualrity://ipns/mydata.net/sub/path ./local/path
```

## Next Steps
Read more documentation and configuration at [README.md](./README.md)

Create a [bug report](https://github.com/tech-greedy/singularity/issues/new?labels=bug&template=bug_report.md&title=) or [request a feature](https://github.com/tech-greedy/singularity/issues/new?labels=enhancement&template=feature_request.md&title=)

