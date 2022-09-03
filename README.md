# singularity

New node software for large-scale clients with PB-scale data onboarding to Filecoin network

![build workflow](https://github.com/tech-greedy/singularity/actions/workflows/node.js.yml/badge.svg)
[![npm version](https://badge.fury.io/js/@techgreedy%2Fsingularity.svg)](https://badge.fury.io/js/@techgreedy%2Fsingularity)

## Quick Start

Looking for standalone Deal Preparation? Try [singularity-prepare](./singularity-prepare.md)

Looking for a complete end-to-end demonstration? Try [Getting Started Guide](./getting-started.md)

### Prerequisite

```shell
# Install nvm (https://github.com/nvm-sh/nvm#install--update-script)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
source ~/.bashrc
# Install node v16
nvm install 16
```

### Install globally from npm

```shell
npm i -g @techgreedy/singularity
singularity -h
```

### Build and run from source

#### 1. Transpile this project

```shell
git clone https://github.com/tech-greedy/singularity.git
cd singularity
npm ci
npm run build
npm link
singularity -h
```

#### 2. Build Dependency

By default, npm will pull the pre-built binaries for dependencies. You can choose to build it from source and override
the one pulled by npm.

```shell
# Make sure you have go v1.17+ installed
git clone https://github.com/tech-greedy/go-generate-car.git
cd go-generate-car
make
```

Then copy the generated binary to override the existing one from the PATH for your node environment, i.e.

* singularity installed globally `/home/user/.nvm/versions/node/v16.xx.x/lib/node_modules/.bin`
* singularity cloned locally `./node_modules/.bin`

Note that the path may change depending on the nodejs version.
If you cannot find the folder above, try searching for the generate-car
binary first (i.e.m `find ~/.nvm -name 'generate-car'`).

## Initialization (Optional)

To use the tool as a daemon, it needs to initialize the config and the database. To do so, run

```shell
singularity init
```

By default, a repository will be initialized at `$HOME_DIR/.singularity`.
Set the environment variable `SINGULARITY_PATH` to override this behavior.

```shell
# Unix
export SINGULARITY_PATH=/the/path/to/the/repo
# Windows
set SINGULARITY_PATH=/the/path/to/the/repo
```

## Topology Choices

Since the tool is modularized, it can be deployed in different ways and have different components enabled or disabled.

Below are configurations for common scenarios.

### Deal Preparation Only

This is useful if you only need deal preparation but not deal making.
You can still have deal making enabled, but disabling it will use slightly less system resources.  
In [default.toml](./config/default.toml) from your repo

1. change `index_service.enabled` to false
2. change `ipfs.enabled` to false
3. change `deal_tracking_service.enabled` to false
4. change `deal_replication_service.enabled` to false
5. change `deal_replication_worker.enabled` to false

### Use External MongoDb database

This is useful if you know MongoDB, and you're hitting some bottlenecks or issues from the built-in MongoDb.

1. Setup your own MongoDb instance
2. In [default.toml](./config/default.toml) from your repo
    1. change `database.start_local` to false
    2. change `connection.database` to the connection string of your own MongoDb database

### Running Workers on different node for Deal Preparation

1. On master server, set `deal_preparation_service.enabled`, `database.start_local` to true and disable all other
   modules
2. On worker servers, set `deal_preparation_worker.enabled` to true and disable all other modules.
   Change `connection.database` and `connection.deal_preparation_service` to the IP address of the master server

## Usage

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

### Start the Daemon

```shell
export SINGULARITY_PATH=/the/path/to/the/repo
singularity daemon
```

### Deal Preparation

Deal preparation contains two parts

* Scanning Request - an initial effort to scan the directory and make plans of how to assign different files and folders
  to different chunks
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
  pause                                                  Pause scanning or generation requests
  resume                                                 Resume scanning or generation requests
  retry                                                  Retry scanning or generation requests
  remove [options] <dataset>                             Remove all records from database for a dataset
  help [command]                                         display help for command
```

#### Create Deal Preparation Request

This will create a scanning request for a dataset. While the dataset is being scanned, it will also produce generation
requests to be taken by workers.

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
  -t, --tmp-dir <tmp_dir>      Optional temporary directory. May be useful when it is at least 2x faster than the dataset source, such as when the dataset is on network mount, and the I/O is the bottleneck
  -f, --skip-inaccessible-files  Skip inaccessible files. Scanning may take longer to complete.
  -m, --min-ratio <min_ratio>  Min ratio of deal to sector size, i.e. 0.55
  -M, --max-ratio <max_ratio>  Max ratio of deal to sector size, i.e. 0.95
  -h, --help                   display help for command
```

#### Support for public S3 bucket

The deal preparation supports public S3 bucket natively. Temporary directory is mandatory when using with S3 bucket.
i.e.

```shell
singularity prep create -t <tmp_dir> <dataset_name> s3://<bucket_name>/<optional_prefix>/ <out_dir>
```

#### Pause/Resume/Retry a request

For each dataset preparation request, it always starts with scanning request, once enough files can be packed into a
single deal, it will create a generation request. In other words, each preparation request is a single scanning request
and a bunch of generation requests.

You can pause/resume/retry the scanning request or generation requests.

```shell
singularity prep pause -h
singularity prep resume -h
singularity prep retry -h
```

#### Remove a request

The whole data preparation requests can be removed from database. All generated CAR files can also be deleted by
specifying `--purge` option.

```shell
singularity prep remove -h
```

#### List Deal Preparation Requests

List all the deal preparation requests, including whether scanning has completed and how many generation requests have
completed or hit errors for each of them.

```shell
singularity prep list
```

#### Check Deal Preparation Request status

Check status for a specific deal preparation request, including the status of the initial scanning request and all
corresponding generation requests.

```shell
singularity prep status -h
```

#### Check specific Deal Generation Request status

Look into a specific generation request, including what are the files or folders included in that request and their
corresponding size, cid, selector, etc.

```shell
singularity prep generation-status -h
```

#### Get Slingshot 3.x Manifest for a Generation Request

```shell
singularity prep generation-manifest -h
```

#### Upload Slingshot 3.x Manifest to web3.storage

```shell
WEB3_STORAGE_TOKEN="eyJ..." singularity prep upload-manifest -h
```

#### Monitor service health and download speed

```shell
singularity monitor
```

### Deal Replication

Deal replication module supports both lotus-market and boost based storage providers (later on we might deprecate
lotus-market support).
Currently it is required to have both lotus and boost cli binary in order for this module to work.

#### Deal Replication Configuration

Look for `default.toml` in the initialized repo, verify in the [deal_replication_worker] section, both binary can be
accessed.
If you need to specify environment variable like FULLNODE_API_INFO, it can also be specified there.

#### Setup Lotus Lite node

In order to make deals, we recommend setting up a [lite node](https://lotus.filecoin.io/lotus/install/lotus-lite/) to
use with the tool.

Once you have the lite node setup, you can import your wallet key for the verified client address.

#### Deal making

```shell
$ singularity repl start -h                                                                 
Usage: singularity replication start [options] <datasetid> <storage-providers> <client> [# of replica]

Start deal replication for a prepared local dataset

Arguments:
  datasetid                                            Existing ID of dataset prepared.
  storage-providers                                    Comma separated storage provider list
  client                                               Client address where deals are proposed from
  # of replica                                         Number of targeting replica of the dataset (default: 10)

Options:
  -u, --url-prefix <urlprefix>                         URL prefix for car downloading. Must be reachable by provider's boostd node. (default: "http://127.0.0.1/")
  -p, --price <maxprice>                               Maximum price per epoch per GiB in Fil. (default: "0")
  -r, --verified <verified>                            Whether to propose deal as verified. true|false. (default: "true")
  -s, --start-delay <startdelay>                       Deal start delay in days. (StartEpoch) (default: "7")
  -d, --duration <duration>                            Duration in days for deal length. (default: "525")
  -o, --offline <offline>                              Propose as offline deal. (default: "true")
  -m, --max-deals <maxdeals>                           Max number of deals in this replication request per SP, per cron triggered. (default: "0")
  -c, --cron-schedule <cronschedule>                   Optional cron to send deals at interval. Use double quote to wrap the format containing spaces.
  -x, --cron-max-deals <cronmaxdeals>                  When cron schedule specified, limit the total number of deals across entire cron, per SP.
  -xp, --cron-max-pending-deals <cronmaxpendingdeals>  When cron schedule specified, limit the total number of pending deals determined by dealtracking service, per SP.
  -h, --help                                           display help for command
```

A simple example to send all car files in one prepared dataset "CommonCrawl" to one storage provider f01234 immediately:

```shell
singularity repl start CommonCrawl f01234 f15djc5avdxihgu234231rfrrzbvnnqvzurxe55kja
```

A more complex example, send 10 deals to storage provider f01234 and f05678, every hour on the 1st minute from prepared
dataset "CommonCrawl", until all CAR files are dealt.

```shell
singularity repl start -m 10 -c "1 * * * *" CommonCrawl f01234,f05678 f15djc5avdxihgu234231rfrrzbvnnqvzurxe55kja
```

## Configuration

Look for `default.toml` in the initialized repo.

### [connection]

#### database

This sets the MongoDb connection string. The default value corresponds to the built-in MongoDb server shipped with this
software.
If you choose to use a standalone MongoDb service, set the connection string here.

#### deal_preparation_service

Sets the API endpoint of deal preparation service.

### [database]

#### start_local

The software is shipping with a built-in MongoDb server. For small to medium-sized dataset, this should be sufficient.

For users who're onboarding large scale datasets, we recommend running your own MongoDb service which fits into your
infrastructure by setting this value to `false`.
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

The default min/max ratio of CAR file size divided by the target deal size. The dataset splitting is performed with
below logic

1. Perform a Glob pattern match and get all files in sorted order
2. Iterate through all the files and keep accumulating file sizes into a chunk
3. Once the size of a chunk is between min and max ratio, pack this chunk to a CAR file and start with a new chunk
4. If the size of the file is too large to fit into a chunk, split the file to hit the min ration

### [deal_preparation_worker]

Worker to scan the dataset, make plan and generate Car file and CIDs

#### enabled, num_workers

Whether to enable the worker and how many worker instances. As a rule of thumb, use `min(cpu_cores / 2, io_MBps / 20)`

## Performance

### Resource usage

Each generation worker consumes negligible RAM, 20-50 MiB/s disk I/O and 100-250% of CPU.

### Speed

Each 32GiB deal takes ~10 minutes to be generated on AMD EPYC CPU with NVME drive.

### Other factors

1. When dealing with lots of small files, CPU usage increases while generation speed decreases.
Meanwhile, IO may become the bottleneck if not using SSD.
2. When using S3 bucket public as the dataset, the Internet Speed may become the bottleneck

## Backup

The repo `~/.singularity` or the folder specified by `SINGULARITY_PATH` contains all state of the service.
To backup, simply backup the repo folder.

## FAQ and common issues

### How to handle inaccessible files

Use `--skip-inaccessible-files` when creating the data preparation request `singularity prep create`.

For existing generation requests, use `singularity prep retry gen --skip-inaccessible-files`,
however this currently only works when the tmpDir is used.

### Does it work on Windows

Only Deal Preparation works and Indexing works on Windows.
Deal Replication and Retrieval only works in Linux/Mac due to dependency restrictions.

### Error - too many open files

In case that one CAR contains more files than allowed by OS, you will need to increase the open file limit with `ulimit`
, or `LimitNOFILE` if using systemd.

### Error: Reached heap limit Allocation failed - JavaScript heap out of memory

Depending on the version, NodeJS by default has a max heap memory of 2GB. To increase this limit, i.e. to increase to
4G, set environment variable
`NODE_OPTIONS="--max-old-space-size=4096"`.

### Error - open /some/file: remote I/O error

If you are using network mount such as NFS or Goofys, a temporary network issue may cause the CAR file generation to
fail.
If the error rate is less than 10%, you may assume they are transient and can be fixed by performing
a retry.
If the error is consistent, you will need to dig into the root cause of what have gone wrong. It could be incorrectly
configured permission or DNS resolver, etc. You can find more details in `/var/log/syslog`.

### Installation failed when using root

Avoid using root, or try the fix below

```shell
chown -R $(whoami) ~/
npm config set unsafe-perm true
npm config set user 0
```

### Error: Instance Exited before being ready and without throwing an error

Something wrong while starting MongoDB. Check what has gone wrong

```shell
MONGOMS_DEBUG=1 singularity daemon
```

If the error shows `libcrypto.so.1.1` cannot be found. Try [this solution](https://stackoverflow.com/a/72633324).

## Submit Feedback

Create a [bug report](https://github.com/tech-greedy/singularity/issues/new?labels=bug&template=bug_report.md&title=)
or [request a feature](https://github.com/tech-greedy/singularity/issues/new?labels=enhancement&template=feature_request.md&title=).
