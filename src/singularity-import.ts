#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';
import ImportOptions from './import/ImportOptions';
import { validateImportOptions } from './import/Util';

const version = packageJson.version;
const program = new Command();
program.name('singularity-import')
  .version(version)
  .description('A tool to automatically import offline deals made by singularity');

program.option('-c, --client <addresses...>', 'List of client addresses to filter the deal proposals', '*')
  .option('-p, --path <paths...>', 'List of paths to find the CAR files')
  .option('-u, --url-prefix <url_prefix>',
    'The URL prefix to download CAR files, if it cannot be found from the specified paths.' +
    ' i.e. using https://www.download.org/ means downloading from https://www.download.org/<cid>.car')
  .addOption(new Option('-j, --download-concurrency <download_concurrency>', 'The number of concurrent threads for downloading')
    .argParser(parseInt)
    .default(8))
  .addOption(new Option('-o, --download-folder <download_folder>', 'The folder to save the downloaded CAR files'))
  .option('-r, --remove-imported', 'Delete the imported CAR files after importing', false)
  .addOption(new Option('-t, --import-interval <interval>',
    'The interval in seconds between imports. ' +
    'The timer starts when the previous import begins so it is possible to have concurrent imports if the interval is too small. ' +
    'For example, if the interval is set to 120s but the miner takes 300s to import a deal, there may be 2-3 imports happening concurrently. ' +
    'Each import will take some RAM and scratch space and can easily get lotus-miner killed due to OOM, ' +
    'so it\'s important to combine this option with --max-concurrent-imports option depending on hardware spec and deal ingestion speed.\n' +
    'The value scales with the size of the deals. For example, if the storage provider sector size is 32GiB and is importing a 16GiB deal, the interval value will be halved.')
    .argParser(parseInt)
    .default(120))
  .addOption(new Option('-m, --max-concurrent-imports <import_concurrency>',
    'This sets an upper limit of concurrent imports when the value from --import-interval is less than the actual time the storage provider spends to import a deal.')
    .argParser(parseInt).default(1))
  .option('-d, --dry-run', 'Do not import deals, just print the deals that would be imported or downloaded', false)
  .option('-l, --loop', 'Keep monitoring the incoming deals and perform the import indefinitely', false)
  .action(async (options: ImportOptions) => {
    validateImportOptions(options);
  });
program.addHelpText('after', `

Environment Variables:
  Make sure you have one of the following environment variables set:
    - LOTUS_MINER_PATH
    - LOTUS_MARKETS_PATH
    - MINER_API_INFO
    - MARKETS_API_INFO

Example Usage:
  - Import all deals continuously one after another:
    $ singularity-import -p /path/to/car -t 0 -m 1 -l
  - Import one deal every 20 minutes with multiple paths
    $ singularity-import -p /path1/to/car -p /path2/to/car -t 1200 -m 1 -l
  - Import one deal every minute with up to 5 concurrent imports
    $ singularity-import -p /path/to/car -t 60 -m 5 -l
  - Import deals from a specific client and the file can be downloaded from their HTTP server. Delete the file after importing.
    $ singularity-import -c f1xxxx -u https://www.download.org/ -o ./downloads -t 0 -m 1 -l -r
`);
program.parse();
