# singularity-prepare
A tool to prepare the dataset for onboarding to Filecoin network

# Quick Start
```shell
# Only works with node v16
npm i -g @techgreedy/singularity
singularity-prepare -h
```
Looking for the daemon version? Try [singularity](./README.md)

# Usage
To parallel the CAR file generation, specify `--parallel`
As a rule of thumb, use `min(cpu_cores / 2.5, io_MBps / 50)`

```shell
$ singularity-prepare -h
Usage: singularity-prepare [options] <datasetName> <datasetPath> <outDir>

A tool to prepare dataset for slingshot

Arguments:
  datasetName                   Name of the dataset
  datasetPath                   Directory path to the dataset
  outDir                        The output Directory to save CAR files and manifest files

Options:
  -V, --version                 output the version number
  -l, --url-prefix <urlPrefix>  The prefix of the download link, which will be followed by datacid.car, i.e. http://download.mysite.org/
  -s, --deal-size <deal_size>   Target deal size, i.e. 32GiB (default: "32 GiB")
  -m, --min-ratio <min_ratio>   Min ratio of deal to sector size, i.e. 0.55 (default: "0.55")
  -M, --max-ratio <max_ratio>   Max ratio of deal to sector size, i.e. 0.95 (default: "0.95")
  -j, --parallel <parallel>     How many generation jobs to run at the same time (default: "1")
  -h, --help                    display help for command
```
