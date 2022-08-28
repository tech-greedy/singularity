#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';
import ImportOptions from './import/ImportOptions';
import ImportUtil from './import/ImportUtil';

const version = packageJson.version;
const program = new Command();
program.name('singularity-import')
  .version(version)
  .description('A tool to automatically import offline deals made by singularity');

program.option('-c, --client <addresses...>', 'List of client addresses to filter the deal proposals', '*')
  .option('-p, --path <paths...>', 'List of paths to find the CAR files')
  .option('-s, --since <seconds_ago>',
    'Import deals that are proposed since this many seconds old and skip those that are older', parseInt, 86400)
  .option('-u, --url-template <url_template>',
    'The URL template to download CAR files, if it cannot be found from the specified paths.' +
    ' i.e. https://www.download.org/{dataCid}.car, https://www.download.org/{pieceCid}.car')
  .option('-dt, --download-threads <download_threads>', 'The number of concurrent threads for downloading',
    parseInt, 8)
  .option('-o, --download-folder <download_folder>', 'The folder to save the downloaded CAR files')
  .option('-r, --remove-imported', 'Delete the imported CAR files after importing', false)
  .option('-i, --interval <interval>',
    'The interval in seconds between handling applicable deals, including both file downloads and deal importing. ' +
    'The timer starts when the previous import begins so it is possible to have concurrent imports or downloads if the interval is too small. ' +
    'For example, if the interval is set to 120s but the miner takes 300s to import a deal, there may be 2-3 imports happening concurrently. ' +
    'Each import will take some RAM and scratch space and can easily get lotus-miner killed due to OOM, ' +
    'so it\'s important to combine this option with --download-concurrency and --import-concurrency option depending on hardware spec and deal ingestion speed.',
    parseInt, 120)
  .option('-I, --interval-cap <interval_cap>',
    'When the next interval arrives and there are already this many concurrent imports or downloads, skip this round and wait for the next interval',
    parseInt, 99)
  .addOption(new Option('-dc, --download-concurrency <download_concurrency>',
    'This sets an upper limit of concurrent downloads')
    .argParser(parseInt).default(1))
  .addOption(new Option('-ic, --import-concurrency <import_concurrency>',
    'This sets an upper limit of concurrent imports')
    .argParser(parseInt).default(1))
  .option('-d, --dry-run', 'Do not import deals, just print the deals that would be imported or downloaded', false)
  .option('-l, --loop', 'Keep monitoring the incoming deals and perform the import indefinitely', false)
  .action(async (options: ImportOptions) => {
    await ImportUtil.startImportLoop(options);
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
    $ singularity-import -p /path/to/car -i 0 -l
  - Import one deal every 20 minutes with multiple paths
    $ singularity-import -p /path1/to/car -p /path2/to/car -i 1200 -l
  - Import one deal every minute with up to 5 concurrent imports
    $ singularity-import -p /path/to/car -i 60 -ic 5 -l
  - Import deals from a specific client and the file can be downloaded from their HTTP server. Delete the file after importing.
    $ singularity-import -c f1xxxx -u https://www.download.org/ -o ./downloads -i 0 -l -r
`);
program.parse();
