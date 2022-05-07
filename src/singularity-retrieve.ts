#!/usr/bin/env node

import Retrieval from './retrieval/Retrieval';
import packageJson from '../package.json';
import { Command } from 'commander';

const version = packageJson.version;
const program = new Command();
program.name('singularity-retrieve')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network');

program.command('ls').description('List the files under a path')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .requiredOption('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .option('-v, --verbose', 'Use a more verbose output')
  .action(async (path, options) => {
    const files = await Retrieval.list(options.ipfsApi, path);
    if (options.verbose) {
      console.table(files);
    } else {
      for (const file of files) {
        console.log(file.name);
      }
    }
    process.exit(0);
  });
program.command('show').description('Show the detailed sources for the corresponding CID and how files are splitted')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .requiredOption('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .action(async (path, options) => {
    const sources = await Retrieval.show(options.ipfsApi, path);
    console.table(sources);
    process.exit(0);
  });
program.command('cp').description('Copy the file from storage provider to local path')
  .argument('<path>', 'The path inside a dataset, i.e. singularity://ipns/dataset.io/path/to/folder')
  .argument('<dest>', 'The destination to save the downloaded file or directory')
  .requiredOption('-p, --provider [providers...]', 'The storage providers to retrieve the data from')
  .requiredOption('--ipfs-api <ipfs_api>', 'The IPFS API to look for dataset index')
  .action(async (path, dest, options) => {
    await Retrieval.cp(options.ipfsApi, path, dest, options.provider);
  });

program.parse();
