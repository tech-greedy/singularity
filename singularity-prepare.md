# singularity-prepare
A tool to prepare the dataset for onboarding to Filecoin network

Looking for the daemon version? Try [singularity](./README.md)

# Quick Start
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
# Build from source
## 1. Transpile this project
```shell
git clone https://github.com/tech-greedy/singularity.git
cd singularity
npm ci
npm run build
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
* singularity installed globally ``/home/user/.nvm/versions/node/v16.xx.x/lib/node_modules/.bin``
* singularity cloned locally `./node_modules/.bin`


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
