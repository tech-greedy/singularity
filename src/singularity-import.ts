#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';

const version = packageJson.version;
const program = new Command();
program.name('singularity-import')
  .version(version)
  .description('A tool to automatically import offline deals made by singularity');

program.option('-c, --client <addresses...>', 'List of client addresses to filter the deal proposals', '*')
  .option('-p, --path <paths...>', 'List of paths to find the CAR files')
  .option('-u, --url-prefix <urlPrefix>',
    'The URL prefix to download CAR files, if it cannot be found from the specified paths.' +
    ' i.e. using https://www.download.org/ means downloading from https://www.download.org/<cid>.car')
  .addOption(new Option('-j, --download-concurrency <concurrency>', 'The number of concurrent threads for downloading')
    .argParser(parseInt)
    .default(8))
  .addOption(new Option('-t, --interval <interval>', 'The interval in seconds between each imports')
    .argParser(parseInt)
    .default(0, 'Import deals continuously'))
  .option('-f, --allow-concurrent-imports',
    'If the above interval has reached and the previous deal has not finished importing, whether to start a new import', false)
  .option('-d, --dry-run', 'Do not import deals, just print the deals that would be imported or downloaded')
  .option('-l, --loop', 'Keep monitoring the incoming deals and perform the import indefinitely')
  .action(async (options) => {
    const clients: string[] = options.client;
    const paths: string[] = options.path;
    const urlPrefix: string = options.urlPrefix;
    const interval: number = options.interval;
    const allowConcurrentImports: boolean = options.allowConcurrentImports;
    console.log({
      clients,
      paths,
      urlPrefix,
      interval,
      allowConcurrentImports
    });
  });
program.parse();
