#!/usr/bin/env node

import Retrieval from './retrieval/Retrieval';
import packageJson from '../package.json';
import { Command } from 'commander';

const version = packageJson.version;
const defaultIpfsApi = '/ip4/127.0.0.1/tcp/5001';
const program = new Command();
program.name('singularity-retrieve')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network');

program.command('ls').description('List the files under a path')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .option('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .option('-v, --verbose', 'Use a more verbose output')
  .option('-j, --json', 'Json output')
  .action(async (path, options) => {
    const retrieval = new Retrieval(options.ipfsApi || defaultIpfsApi);
    const files = await retrieval.list(path, options.verbose);
    if (options.json) {
      console.log(JSON.stringify(files));
    } else {
      console.table(files);
    }
    process.exit(0);
  });
program.command('explain').description('Explain the detailed sources for the corresponding CID and how files are splitted')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .option('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .option('-j, --json', 'Json output')
  .action(async (path, options) => {
    const retrieval = new Retrieval(options.ipfsApi || defaultIpfsApi);
    const [sources, stat] = await retrieval.explain(path);
    if (options.json) {
      console.log(JSON.stringify({ sources, stat }));
    } else {
      console.table(stat);
      console.table(sources);
    }
    process.exit(0);
  });
program.command('cp').description('Copy the file from storage provider to local path')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .argument('<dest>', 'The destination to save the downloaded file or directory')
  .requiredOption('-p, --provider [providers...]', 'The storage providers to retrieve the data from')
  .option('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .action(async (path, dest, options) => {
    const retrieval = new Retrieval(options.ipfsApi || defaultIpfsApi);
    await retrieval.cp(path, dest, options.provider);
    process.exit(0);
  });

program.parse();
