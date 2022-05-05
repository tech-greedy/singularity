#!/usr/bin/env node
/* eslint-disable import/first */
import { homedir } from 'os';
import path from 'path';
import { Command } from 'commander';
import packageJson from '../package.json';
import Datastore from './common/Datastore';
import DealPreparationService from './deal-preparation/DealPreparationService';
import DealPreparationWorker from './deal-preparation/DealPreparationWorker';
import axios, { AxiosResponse } from 'axios';
import config from 'config';
import CliUtil from './cli-util';
import IndexService from './index/IndexService';
import HttpHostingService from './hosting/HttpHostingService';
import DealTrackingService from './deal-tracking/DealTrackingService';
import GetPreparationDetailsResponse from './deal-preparation/GetPreparationDetailsResponse';
import fs from 'fs-extra';
import Logger, { Category } from './common/Logger';
import GenerationRequest from './common/model/GenerationRequest';

process.env.NODE_CONFIG_DIR = process.env.SINGULARITY_PATH || path.join(homedir(), '.singularity');

const logger = Logger.getLogger(Category.Cli);
const version = packageJson.version;
const program = new Command();
program.name('singularity')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network');

program.command('init')
  .description('Initialize the configuration directory')
  .action(async () => {
    const configDir = config.util.getEnv('NODE_CONFIG_DIR');
    await fs.mkdirp(configDir);
    if (!await fs.pathExists(path.join(configDir, 'default.toml'))) {
      logger.info(`Initializing at ${configDir} ...`);
      await fs.copyFile(path.join(__dirname, '../config/default.toml'), path.join(configDir, 'default.toml'));
    } else {
      logger.warn(`${configDir} already has the repo.`);
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
      if (config.get('deal_preparation_worker.enable_cleanup')) {
        const outDir = path.resolve(process.env.NODE_CONFIG_DIR!, config.get('deal_preparation_worker.out_dir'));
        if (await fs.pathExists(outDir)) {
          for (const file of await fs.readdir(outDir)) {
            const regex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.car$/;
            if (regex.test(file)) {
              const fullPath = path.join(outDir, file);
              logger.info(`Removing temporary file ${fullPath}`);
              await fs.remove(fullPath);
            }
          }
        }
      }
      if (config.get('deal_preparation_worker.enabled')) {
        const numWorkers = config.has('deal_preparation_worker.num_workers') ? config.get<number>('deal_preparation_worker.num_workers') : 1;
        for (let i = 0; i < numWorkers; ++i) {
          new DealPreparationWorker().start();
        }
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

const index = program.command('index').description('Manage the dataset indexing');
index.command('create')
  .argument('<id>', 'A unique id of the dataset')
  .action((id) => {
    const url: string = config.get('connection.index_service');
    axios.get(`${url}/create/${id}`).then(CliUtil.renderResponseOld).catch(CliUtil.renderErrorAndExit);
  });

const preparation = program.command('preparation')
  .alias('prep')
  .description('Start preparation for a local dataset');

preparation.command('start')
  .argument('<name>', 'A unique name of the dataset')
  .argument('<path>', 'Directory path to the dataset')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 16GiB', '32 GiB')
  .action(async (name, p, options) => {
    if (!await fs.pathExists(p)) {
      logger.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    const dealSize: string = options.dealSize;
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = await axios.post(`${url}/preparation`, {
        name: name,
        path: path.resolve(p),
        dealSize: dealSize
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('status')
  .option('--json', 'Output with JSON format')
  .argument('<id>', 'A unique id of the dataset')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = await axios.get(`${url}/preparation/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    const data : GetPreparationDetailsResponse = response.data;
    if (options.json) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      const { generationRequests, ...summary } = data;
      console.log('Scanning Request Summary');
      console.table([summary]);
      console.log('Corresponding Generation Requests');
      console.table(generationRequests);
    }
  });

preparation.command('list')
  .option('--json', 'Output with JSON format')
  .action(async (options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = await axios.get(`${url}/preparations`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('generation-status')
  .option('--json', 'Output with JSON format')
  .argument('<id>', 'A unique id of the generation request')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = await axios.get(`${url}/generation/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    const data = <GenerationRequest> response.data;
    if (options.json) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      const { fileList, ...summary } = data;
      console.log('Generation Request Summary');
      console.table([summary]);
      console.log('File Lists');
      console.table(fileList);
    }
  });

preparation.command('pause')
  .argument('<id>', 'A unique id of the dataset')
  .action(async (id) => {
    const url: string = config.get('connection.deal_preparation_service');
    try {
      await axios.post(`${url}/preparation/${id}`, { status: 'paused' });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    console.log('Success');
  });

preparation.command('resume')
  .argument('<id>', 'A unique id of the dataset')
  .action(async (id) => {
    const url: string = config.get('connection.deal_preparation_service');
    try {
      await axios.post(`${url}/preparation/${id}`, { status: 'active' });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    console.log('Success');
  });

program.parse();
