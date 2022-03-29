#!/usr/bin/env node
import { Command } from 'commander';
import packageJson from '../package.json';
import fs from 'fs';
import Datastore from './common/Datastore';
import DealPreparationService from './deal-preparation/DealPreparationService';
import DealPreparationWorker from './deal-preparation/DealPreparationWorker';
import axios from 'axios';
import { NODE_CONFIG_DIR } from './env';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const config = require('config');

console.log(`Using Singularity config directory ${config.util.getEnv('NODE_CONFIG_DIR')}`);
console.log(`Was set to ${NODE_CONFIG_DIR}`);
const version = packageJson.version;
const program = new Command();
program.name('singularity')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network');

program.command('daemon')
  .description('Start a daemon process for deal preparation and deal making')
  .action((_options) => {
    (async function () {
      await Datastore.init();
      if (config.get('deal_preparation_service.enabled')) {
        new DealPreparationService().start();
      }
      if (config.get('deal_preparation_worker.enabled')) {
        new DealPreparationWorker().start();
      }
    })();
  });

const preparation = program.command('preparation')
  .alias('prep')
  .description('Start preparation for a local dataset');

preparation.command('start')
  .argument('<name>', 'A unique name of the dataset')
  .argument('<path>', 'Directory path to the dataset')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 16GiB', '32 GiB')
  .action((name, path, options) => {
    if (!fs.existsSync(path)) {
      console.error(`Dataset path "${path}" does not exist.`);
      process.exit(1);
    }
    const dealSize: string = options.dealSize;
    const url: string = config.get('connection.deal_preparation_service');
    axios.post(`${url}/preparation`, {
      name: name,
      path: path,
      dealSize: dealSize
    }).then(_response => {
      console.log('response');
    }).catch(_error => {
      console.error('error');
      process.exit(1);
    });
  });

program.parse();
