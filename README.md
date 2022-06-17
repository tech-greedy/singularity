# singularity
New node software for large-scale clients with PB-scale data onboarding to Filecoin network

![build workflow](https://github.com/tech-greedy/singularity/actions/workflows/node.js.yml/badge.svg)
[![npm version](https://badge.fury.io/js/@techgreedy%2Fsingularity.svg)](https://badge.fury.io/js/@techgreedy%2Fsingularity)

# Quick Start
Looking for standalone Deal Preparation? Try [singularity-prepare](./singularity-prepare.md)
## Prerequisite
```shell
# Install nvm (https://github.com/nvm-sh/nvm#install--update-script)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
source ~/.bashrc
# Install node v16
nvm install 16
```
# Install globally from npm
```shell
npm i -g @techgreedy/singularity
singularity-prepare -h
```
# Build and run from source
## 1. Transpile this project
```shell
git clone https://github.com/tech-greedy/singularity.git
cd singularity
npm ci
npm run build-singularity
npm run build-singularity-retrieve
npm run build-singularity-prepare
npm link
npx singularity-prepare
```
## 2. Build Dependency
By default, npm will pull the pre-built binaries for dependencies. You can choose to build it from source and override the one pulled by npm.
```shell
# Make sure you have go v1.17+ installed
git clone https://github.com/tech-greedy/go-generate-car.git
cd go-generate-car
make
```
Then copy the generated binary to override the existing one from the PATH for your node environment, i.e.
* singularity installed globally `/home/user/.nvm/versions/node/v16.15.0/bin`
* singularity cloned locally `./node_modules/.bin`


# Initialization
To use the tool as a daemon, it needs to initialize the config and the database. To do so, run
```shell
singularity init
```
By default a repository will be initialized at `$HOME_DIR/.singularity`. 
Set the environment variable `SINGULARITY_PATH` to override this behavior.
```shell
# Unix
export SINGULARITY_PATH=/the/path/to/the/repo
# Windows
set SINGULARITY_PATH=/the/path/to/the/repo
```

# Deployment
Since the tool is modularized, it can be deployed in different ways and have different components enabled or disabled.

Below are configurations for common scenarios.
## Deal Preparation Only
This is useful if you only need deal preparation but not deal making.
You can still have deal making enabled, but disabling it will use slightly less system resources.  
In [default.toml](./config/default.toml) from your repo
1. change `index_service.enabled` to false
2. change `ipfs.enabled` to false
3. change `http_hosting_service.enabled` to false
4. change `hdeal_tracking_service.enabled` to false

## Use External MongoDb database
This is useful if you know MongoDB, and you're hitting some bottlenecks or issues from the built-in MongoDb.
1. Setup your own MongoDb instance
2. In [default.toml](./config/default.toml) from your repo
   1. change `database.start_local` to false
   2. change `connection.database` to the connection string of your own MongoDb database


# Usage
```shell
$ singularity
Usage: singularity [options] [command]

A tool for large-scale clients with PB-scale data onboarding to Filecoin network
Visit https://github.com/tech-greedy/singularity for more details

Options:
  -V, --version     output the version number
  -h, --help        display help for command

Commands:
  init              Initialize the configuration directory in SINGULARITY_PATH
                    If unset, it will be initialized at HOME_DIR/.singularity
  daemon            Start a daemon process for deal preparation and deal making
  index             Manage the dataset index which will help map the dataset path to actual piece
  preparation|prep  Manage deal preparation
  help [command]    display help for command
```

## Deal Preparation
Deal preparation contains two parts
* Scanning Request - an initial effort to scan the directory and make plans of how to assign different files and folders to different chunks
* Generation Request - subsequent works to generate the car file and compute the commP
```shell
$ singularity prep -h
Usage: singularity preparation|prep [options] [command]

Manage deal preparation

Options:
  -h, --help                                             display help for command

Commands:
  create [options] <datasetName> <datasetPath> <outDir>  Start deal preparation for a local dataset
  status [options] <dataset>                             Check the status of a deal preparation request
  list [options]                                         List all deal preparation requests
  generation-manifest [options] <generationId>           Get the Slingshot v3.x manifest data for a single deal generation request
  generation-status [options] <generationId>             Check the status of a single deal generation request
  pause [options] <dataset> [generationId]               Pause an active deal preparation request and its active deal generation requests
  resume [options] <dataset> [generationId]              Resume a paused deal preparation request and its paused deal generation requests
  retry [options] <dataset> [generationId]               Retry an errored preparation request and its errored deal generation requests
  remove [options] <dataset>                             Remove all records from database for a dataset
  help [command]                                         display help for command
```

### Create Deal Preparation Request
This will create a scanning request for a dataset. While the dataset is being scanned, it will also produce generation requests to be taken by workers.
```shell
$ singularity prep create -h
Usage: singularity preparation create [options] <datasetName> <datasetPath> <outDir>

Start deal preparation for a local dataset

Arguments:
  datasetName                  A unique name of the dataset
  datasetPath                  Directory path to the dataset
  outDir                       The output Directory to save CAR files

Options:
  -s, --deal-size <deal_size>  Target deal size, i.e. 32GiB (default: "32 GiB")
  -m, --min-ratio <min_ratio>  Min ratio of deal to sector size, i.e. 0.55
  -M, --max-ratio <max_ratio>  Max ratio of deal to sector size, i.e. 0.95
  -h, --help                   display help for command
```
### Pause/Resume a request
You can pause the entire deal preparation or a specific generation request. However, all ongoing generation requests taken by the workers will not be paused.
```shell
$ singularity prep pause -h
$ singularity prep resume -h
```
### Retry a request
Sometimes, the request may fail due to reasons such as I/O error. Once they hit the error state, you can choose to retry those requests after you've solved underlying issues.
```shell
$ singularity prep retry -h
```
### Remove a request
The whole data preparation requests can be removed from database. All generated CAR files can also be deleted by specifying `--purge` option.
```shell
$ singularity prep remove -h
```
### List Deal Preparation Requests
List all the deal preparation requests, including whether scanning has completed and how many generation requests have completed or hit errors for each of them.
```shell
$ singularity prep list
```
### Check Deal Preparation Request status
Check status for a specific deal preparation request, including the status of the initial scanning request and all corresponding generation requests.
```shell
$ singularity prep status -h
```
### Check specific Deal Generation Request status
Look into a specific generation request, including what are the files or folders included in that request and their corresponding size, cid, selector, etc.
```shell
$ singularity prep generation-status -h
```
### Get Slingshot 3.x Manifest for a Generation Request
```shell
$ singularity prep generation-manifest -h
```

## Configuration
Look for `default.toml` in the initialized repo

### [connection]
#### database
This sets the MongoDb connection string. The default value corresponds to the built-in MongoDb server shipped with this software.
If you choose to use a standalone MongoDb service, set the connection string here.
#### deal_preparation_service
Sets the API endpoint of deal preparation service.

### [database]
#### start_local
The software is shipping with a built-in MongoDb server. For small to medium-sized dataset, this should be sufficient.

For users who're onboarding large scale datasets, we recommend running your own MongoDb service which fits into your infrastructure by setting this value to `false`.
To connect to a standalone MongoDb service, set the value of connection string [here](#database).

Not that the MongoDB server may consume as much as 80% of usable memory.
#### local_path, local_bind, local_port
The path of the database files the built-in MongoDb will be using, as well as the IP and port to bind the service to.

### [deal_preparation_service]
Service to manage preparation requests

#### enabled, bind, port
Whether to enable the service and which IP and port to bind the service to

#### enable_cleanup
If the service crashes or is interrupted, there may be incomplete CAR files generated. Enabling this can clean them up.

#### minDealSizeRatio, maxDealSizeRatio
The default min/max ratio of CAR file size divided by the target deal size. The dataset splitting is performed with below logic
1. Perform a Glob pattern match and get all files in sorted order
2. Iterate through all the files and keep accumulating file sizes into a chunk
3. Once the size of a chunk is between min and max ratio, pack this chunk to a CAR file and start with a new chunk
4. If the size of the file is too large to fit into a chunk, split the file to hit the min ration

### [deal_preparation_worker]
Worker to scan the dataset, make plan and generate Car file and CIDs

#### enabled, num_workers
Whether to enable the worker and how many worker instances. As a rule of thumb, use `min(cpu_cores / 2.5, io_MBps / 50)`

# Build from source
## This project
You can transpile this project with npm
```shell
# Make sure you have npm v16 installed, i.e. via nvm (https://github.com/nvm-sh/nvm)
git clone https://github.com/tech-greedy/singularity.git
cd singularity
npm ci
npm run build-singularity
npm run build-singularity-retrieve
npm run build-singularity-prepare
npm link
singularity -h
```
The dependency of this project can be built from source
## go-generate-car
```shell
# Make sure you have go v1.17+ installed
git clone https://github.com/tech-greedy/go-generate-car.git
cd go-generate-car
make
```
Then copy the generated binary to override the existing one from the PATH for your node environment, i.e. 
* singularity installed globally `/home/user/.nvm/versions/node/v16.15.0/bin`
* singularity cloned locally `./node_modules/.bin`

# FAQ
### Does it work in Windows
Only Deal Preparation works and Indexing works in Windows.
Deal Replication and Retrieval only works in Linux/Mac due to dependency restrictions. 

