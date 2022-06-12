# singularity
New node software for large-scale clients with PB-scale data onboarding to Filecoin network

![build workflow](https://github.com/tech-greedy/singularity/actions/workflows/node.js.yml/badge.svg)
[![npm version](https://badge.fury.io/js/@techgreedy%2Fsingularity.svg)](https://badge.fury.io/js/@techgreedy%2Fsingularity)

# Quick Start
```shell
npm i -g @techgreedy/singularity
singularity init
singularity daemon
singularity prep create -h
```

# Initialization
By default a repository will be initialized at `$HOME_DIR/.singularity`. 
Set the environment variable `SINGULARITY_PATH` to override this behavior.
```shell
# Unix
export SINGULARITY_PATH=/the/path/to/the/repo
# Windows
set SINGULARITY_PATH=/the/path/to/the/repo
```

## Configuration
The [default config](./config/default.toml) is copied over to the repo.

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

#### minDealSizeRatio, maxDealSizeRatio
The min/max ratio of CAR file size divided by the target deal size. The dataset splitting is performed with below logic
1. Perform a Glob pattern match and get all files in sorted order
2. Iterate through all the files and keep accumulating file sizes into a chunk
3. Once the size of a chunk is between min and max ratio, pack this chunk to a CAR file and start with a new chunk
4. If the size of the file is too large to fit into a chunk, split the file to hit the min ration

### [deal_preparation_worker]
Worker to scan the dataset, make plan and generate Car file and CIDs

#### enabled, num_workers
Whether to enable the worker and how many worker instances. As a rule of thumb, use `min(cpu_cores / 2.5, io_MBps / 50)`

#### enable_cleanup
If the service crashes or is interrupted, there may be incomplete CAR files generated. Enabling this can clean them up.

#### out_dir
This will be deprecated

# Deployment
Since the tool is modularized, it can be deployed in different ways and have different components enabled or disabled.

Below are configurations for commonly used deployment topology.
## Use Standalone MongoDb database
1. Setup your own MongoDb instance
2. In [default.toml](./config/default.toml) from your repo
   1. change `database.start_local` to false
   2. change `connection.database` to the connection string of your own MongoDb database
### Deal Preparation Only
In [default.toml](./config/default.toml) from your repo
1. change `index_service.enabled` to false
2. change `ipfs.enabled` to false
3. change `http_hosting_service.enabled` to false
4. change `hdeal_tracking_service.enabled` to false


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
```shell
$ singularity prep -h
Usage: singularity preparation|prep [options] [command]

Manage deal preparation

Options:
  -h, --help                                    display help for command

Commands:
  create [options] <datasetName> <datasetPath>  Start deal preparation for a local dataset
  status [options] <dataset>                    Check the status of a deal preparation request
  list [options]                                List all deal preparation requests
  generation-status [options] <generationId>    Check the status of a single deal generation request
  pause [options] <dataset> [generationId]      Pause an active deal preparation request and its active deal generation requests
  resume [options] <dataset> [generationId]     Resume a paused deal preparation request and its paused deal generation requests
  retry [options] <dataset> [generationId]      Retry an errored preparation request and its errored deal generation requests
  remove [options] <dataset>                    Remove all records from database for a dataset
  help [command]                                display help for command
```

# FAQ
### Does it work in Windows
Only Deal Preparation works and Indexing works in Windows.
Deal Replication and Retrieval only works in Linux/Mac due to dependency restrictions. 

