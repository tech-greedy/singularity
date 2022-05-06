#!/usr/bin/env node
/* eslint-disable import/first */
import { homedir } from 'os';
import path from 'path';
process.env.NODE_CONFIG_DIR = process.env.SINGULARITY_PATH || path.join(homedir(), '.singularity');
import { Argument, Command } from 'commander';
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

const logger = Logger.getLogger(Category.Cli);
const version = packageJson.version;
const program = new Command();
program.name('singularity')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network\nVisit https://github.com/tech-greedy/singularity for more details');

program.command('init')
  .description('Initialize the configuration directory in SINGULARITY_PATH\nIf unset, it will be initialized at HOME_DIR/.singularity')
  .action(async () => {
    const configDir = config.util.getEnv('NODE_CONFIG_DIR');
    await fs.mkdirp(configDir);
    if (!await fs.pathExists(path.join(configDir, 'default.toml'))) {
      logger.info(`Initializing at ${configDir} ...`);
      await fs.copyFile(path.join(__dirname, '../config/default.toml'), path.join(configDir, 'default.toml'));
      logger.info(`Please check ${path.join(configDir, 'default.toml')}`);
    } else {
      logger.warn(`${configDir} already has the repo.`);
    }
  });

program.command('daemon')
  .description('Start a daemon process for deal preparation and deal making')
  .action((_options) => {
    (async function () {
      let indexService: IndexService;
      process.on('SIGUSR2', async () => {
        // Gracefully turn off mongodb memory server
        if (Datastore['mongoMemoryServer']) {
          await Datastore['mongoMemoryServer'].stop();
        }
        // unlock ipfs repo
        if (config.get('index_service.enabled') && config.get('index_service.start_ipfs') && indexService && indexService['ipfsClient']) {
          await indexService['ipfsClient'].stop();
        }

        process.kill(process.pid, 'SIGKILL');
      });
      await Datastore.init(false);
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
        indexService = new IndexService();
        await indexService.init();
        indexService.start();
      }
      if (config.get('http_hosting_service.enabled')) {
        new HttpHostingService().start();
      }
      if (config.get('deal_tracking_service.enabled')) {
        new DealTrackingService().start();
      }
    })();
  });

const index = program.command('index').description('Manage the dataset index which will help map the dataset path to actual piece');
index.command('create')
  .argument('<id_or_name>', 'The dataset id or name')
  .action(async (id) => {
    const url: string = config.get('connection.index_service');
    let response! : AxiosResponse;
    try {
      response = await axios.get(`${url}/create/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    const cid: string = response.data.rootCid;
    console.log('To publish the index to IPNS:');
    console.log(`  ipfs name publish /ipfs/${cid}`);
    console.log('To publish the index to DNSLink:');
    console.log('  Add or update the TXT record for _dnslink.your_domain.net');
    console.log(`  _dnslink.your_domain.net.  34  IN  TXT "dnslink=/ipfs/${cid}"`);
  });

const preparation = program.command('preparation')
  .alias('prep')
  .description('Manage deal preparation');

preparation.command('create').description('Start deal preparation for a local dataset')
  .argument('<datasetName>', 'A unique name of the dataset')
  .argument('<datasetPath>', 'Directory path to the dataset')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 32GiB', '32 GiB')
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

preparation.command('status').description('Check the status of a deal preparation request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
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

preparation.command('list').description('List all deal preparation requests')
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

preparation.command('generation-status').description('Check the status of a single deal generation request')
  .option('--json', 'Output with JSON format')
  .option('--dataset <dataset>', 'The dataset id or name, required if looking for generation request using index')
  .argument('<generationId>', 'A unique id or index of the generation request')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = options.dataset ? await axios.get(`${url}/generation/${options.dataset}/${id}`) : await axios.get(`${url}/generation/${id}`);
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

async function UpdateState (id : string, generation: string, action: string) : Promise<AxiosResponse> {
  const url: string = config.get('connection.deal_preparation_service');
  let response! : AxiosResponse;
  try {
    if (generation) {
      response = await axios.post(`${url}/preparation/${id}/${generation}`, { action });
    } else {
      response = await axios.post(`${url}/preparation/${id}`, { action });
    }
  } catch (error) {
    CliUtil.renderErrorAndExit(error);
  }
  return response;
}

preparation.command('pause').description('Pause an active deal preparation request and its active deal generation requests')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generationId>', 'Optionally specify a generation request').argOptional())
  .action(async (id, generation, options) => {
    const response = await UpdateState(id, generation, 'pause');
    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('resume').description('Resume a paused deal preparation request and its paused deal generation requests')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generationId>', 'Optionally specify a generation request').argOptional())
  .action(async (id, generation, options) => {
    const response = await UpdateState(id, generation, 'resume');
    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('retry').description('Retry an errored preparation request and its errored deal generation requests')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generationId>', 'Optionally specify a generation request').argOptional())
  .action(async (id, generation, options) => {
    const response = await UpdateState(id, generation, 'retry');
    CliUtil.renderResponse(response.data, options.json);
  });

program.showSuggestionAfterError();
program.showHelpAfterError('(add --help for additional information)');
program.parse();
