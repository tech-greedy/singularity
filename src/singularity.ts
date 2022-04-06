#!/usr/bin/env node
import { Command } from 'commander';
import packageJson from '../package.json';
import fs from 'fs';
import Datastore from './common/Datastore';
import DealPreparationService from './deal-preparation/DealPreparationService';
import DealPreparationWorker from './deal-preparation/DealPreparationWorker';
import axios from 'axios';
import config from 'config';
import path from 'path';
import CliUtil from './cli-util';
import IndexService from './index/IndexService';
import HttpHostingService from './hosting/HttpHostingService';
import DealTrackingService from './deal-tracking/DealTrackingService';

const version = packageJson.version;
const program = new Command();
program.name('singularity')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network');

program.command('init')
  .description('Initialize the configuration directory')
  .action(() => {
    const configDir = config.util.getEnv('NODE_CONFIG_DIR');
    fs.mkdirSync(configDir, { recursive: true });
    if (!fs.existsSync(path.join(configDir, 'default.toml'))) {
      console.info(`Initializing at ${configDir} ...`);
      fs.copyFileSync(path.join(__dirname, '../config/default.toml'), path.join(configDir, 'default.toml'));
    }
  });

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
      if (config.get('index_service.enabled')) {
        new IndexService().start();
      }
      if (config.get('http_hosting_service.enabled')) {
        new HttpHostingService().start();
      }
      if (config.get('deal_tracking_service.enabled')) {
        new DealTrackingService().start();
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
  .action((name, p, options) => {
    if (!fs.existsSync(p)) {
      console.error(`Dataset path "${path}" does not exist.`);
      process.exit(1);
    }
    const dealSize: string = options.dealSize;
    const url: string = config.get('connection.deal_preparation_service');
    axios.post(`${url}/preparation`, {
      name: name,
      path: path.resolve(p),
      dealSize: dealSize
    }).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

preparation.command('status')
  .argument('<id>', 'A unique id of the dataset')
  .action((id) => {
    const url: string = config.get('connection.deal_preparation_service');
    axios.get(`${url}/preparation/${id}`).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

preparation.command('list')
  .action(() => {
    const url: string = config.get('connection.deal_preparation_service');
    axios.get(`${url}/preparations`).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

preparation.command('generation-status')
  .argument('<id>', 'A unique id of the generation request')
  .action((id) => {
    const url: string = config.get('connection.deal_preparation_service');
    axios.get(`${url}/generation/${id}`).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

preparation.command('pause')
  .argument('<id>', 'A unique id of the dataset')
  .action((id) => {
    const url: string = config.get('connection.deal_preparation_service');
    axios.post(`${url}/preparation/${id}`, { status: 'paused' }).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

preparation.command('resume')
  .argument('<id>', 'A unique id of the dataset')
  .action((id) => {
    const url: string = config.get('connection.deal_preparation_service');
    axios.post(`${url}/preparation/${id}`, { status: 'active' }).then(CliUtil.renderResponse).catch(CliUtil.renderErrorAndExit);
  });

program.parse();
