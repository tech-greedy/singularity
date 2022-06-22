#!/usr/bin/env node
/* eslint-disable import/first */
import { homedir } from 'os';
import path from 'path';
import cluster from 'node:cluster';
process.env.NODE_CONFIG_DIR = process.env.SINGULARITY_PATH || path.join(homedir(), '.singularity');
import { Argument, Command, Option } from 'commander';
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
import { Worker } from 'cluster';
import * as IpfsCore from 'ipfs-core';
import DealReplicationService from './replication/DealReplicationService';
import DealReplicationWorker from './replication/DealReplicationWorker';
import GenerateCar from './common/GenerateCar';

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
      GenerateCar.initialize();
      if (cluster.isMaster) {
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
        await Datastore.connect();
        if (config.get('ipfs.enabled')) {
          await IpfsCore.create();
        }
        const workers: [Worker, string][] = [];
        let readied = 0;
        cluster.on('message', _ => {
          readied += 1;
          if (readied === workers.length) {
            for (const w of workers) {
              w[0].send(w[1]);
            }
          }
        });
        if (config.get('deal_preparation_service.enabled')) {
          workers.push([cluster.fork(), 'deal_preparation_service']);
        }
        if (config.get('deal_preparation_worker.enabled')) {
          const numWorkers = config.has('deal_preparation_worker.num_workers') ? config.get<number>('deal_preparation_worker.num_workers') : 1;
          for (let i = 0; i < numWorkers; ++i) {
            workers.push([cluster.fork(), 'deal_preparation_worker']);
          }
        }
        if (config.get('index_service.enabled')) {
          workers.push([cluster.fork(), 'index_service']);
        }
        if (config.get('http_hosting_service.enabled')) {
          workers.push([cluster.fork(), 'http_hosting_service']);
        }
        if (config.get('deal_tracking_service.enabled')) {
          workers.push([cluster.fork(), 'deal_tracking_service']);
        }
        if (config.get('deal_replication_service.enabled')) {
          workers.push([cluster.fork(), 'deal_replication_service']);
        }
        if (config.get('deal_replication_worker.enabled')) {
          workers.push([cluster.fork(), 'deal_replication_worker']);
        }
      } else if (cluster.isWorker) {
        await Datastore.connect();
        process.on('message', async (msg) => {
          switch (msg) {
            case 'deal_preparation_service':
              new DealPreparationService().start();
              break;
            case 'deal_preparation_worker':
              new DealPreparationWorker().start();
              break;
            case 'index_service':
              new IndexService().start();
              break;
            case 'http_hosting_service':
              new HttpHostingService().start();
              break;
            case 'deal_tracking_service':
              new DealTrackingService().start();
              break;
            case 'deal_replication_service':
              new DealReplicationService().start();
              break;
            case 'deal_replication_worker':
              new DealReplicationWorker().start();
              break;
          }
        });
        process.send!('ready');
      }
    })();
  });

const index = program.command('index').description('Manage the dataset index which will help map the dataset path to actual piece');
index.command('create')
  .argument('<id_or_name>', 'The dataset id or name')
  .action(async (id) => {
    const url: string = config.get('connection.index_service');
    let response!: AxiosResponse;
    try {
      response = await axios.get(`${url}/create/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    const cid: string = response.data.rootCid;
    if (response.data.warning) {
      console.warn(response.data.warning);
    }
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
  .argument('<outDir>', 'The output Directory to save CAR files')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 32GiB', '32 GiB')
  .option('-t, --tmp-dir <tmp_dir>', 'Temporary directory, may be useful when dataset source is slow, such as on S3 mount or NFS')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').argParser(parseFloat))
  .action(async (name, p, outDir, options) => {
    if (!await fs.pathExists(p)) {
      logger.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    const dealSize: string = options.dealSize;
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = await axios.post(`${url}/preparation`, {
        name: name,
        path: path.resolve(p),
        dealSize: dealSize,
        outDir: path.resolve(outDir),
        minRatio: options.minRatio,
        maxRatio: options.maxRatio,
        tmpDir: options.tmpDir ? path.resolve(options.tmpDir) : undefined
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
    let response!: AxiosResponse;
    try {
      response = await axios.get(`${url}/preparation/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    const data: GetPreparationDetailsResponse = response.data;
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
    let response!: AxiosResponse;
    try {
      response = await axios.get(`${url}/preparations`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('generation-manifest').description('Get the Slingshot v3.x manifest data for a single deal generation request')
  .option('--dataset <dataset>', 'The dataset id or name, required if looking for generation request using index')
  .option('--pretty', 'Whether to add indents to output JSON')
  .requiredOption('-l, --url-prefix <urlPrefix>', 'The prefix of the download link, which will be followed by datacid.car, i.e. http://download.mysite.org/')
  .argument('<generationId>', 'A unique id or index of the generation request')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response! : AxiosResponse;
    try {
      response = options.dataset ? await axios.get(`${url}/generation-manifest/${options.dataset}/${id}`) : await axios.get(`${url}/generation-manifest/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    const data = response.data;
    if (!options.urlPrefix.endsWith('/')) {
      options.urlPrefix = options.urlPrefix + '/';
    }
    data.car_file_link = options.urlPrefix + data.payload_cid + '.car';
    if (options.pretty) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      console.log(JSON.stringify(data));
    }
  });

preparation.command('generation-status').description('Check the status of a single deal generation request')
  .option('--json', 'Output with JSON format')
  .option('--dataset <dataset>', 'The dataset id or name, required if looking for generation request using index')
  .argument('<generationId>', 'A unique id or index of the generation request')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = options.dataset ? await axios.get(`${url}/generation/${options.dataset}/${id}`) : await axios.get(`${url}/generation/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    const data = response.data;
    if (options.json) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      const { fileList, generatedFileList, ...summary } = data;
      console.log('Generation Request Summary');
      console.table([summary]);
      console.log('File Lists');
      console.table(fileList.length > 0 ? fileList : generatedFileList);
    }
  });

async function UpdateState (id: string, generation: string, action: string): Promise<AxiosResponse> {
  const url: string = config.get('connection.deal_preparation_service');
  let response!: AxiosResponse;
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

preparation.command('remove').description('Remove all records from database for a dataset')
  .option('--purge', 'Whether to also purge the car files')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id, options) => {
    const url: string = config.get('connection.deal_preparation_service');
    try {
      await axios.delete(`${url}/preparation/${id}`, { data: { purge: options.purge } });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
  });

const replication = program.command('replication')
  .alias('repl')
  .description('Start replication for a local dataset');

replication.command('start')
  .description('Start deal replication for a prepared local dataset')
  .argument('<datasetid>', 'Existing ID of dataset prepared.')
  .argument('<# of replica>', 'Number of targeting replica of the dataset')
  .argument('<criteria>', 'Comma separated miner list')
  .argument('<client>', 'Client address where deals are proposed from')
  .option('-u, --url-prefix <urlprefix>', 'URL prefix for car downloading. Must be reachable by provider\'s boostd node.', 'http://127.0.0.1/')
  .option('-p, --price <maxprice>', 'Maximum price per epoch per GiB in Fil.', '0.000000002')
  .option('-r, --verified <verified>', 'Whether to propose deal as verified. true|false.', 'true')
  .option('-d, --duration <duration>', 'Duration in days for deal length.', '500')
  .option('-o, --offline <offline>', 'Propose as offline deal.', 'true')
  .option('-m, --max-deals <maxdeals>', 'Max number of deals in this replication request per SP.', '0')
  .action(async (datasetid, replica, criteria, client, options) => {
    let response!: AxiosResponse;
    try {
      console.log(options);
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication`, {
        datasetId: datasetid,
        replica: replica,
        criteria: criteria,
        client: client,
        urlPrefix: options.urlPrefix,
        maxPrice: options.price,
        isVerfied: options.verified,
        duration: options.duration * 2880, // convert to epoch
        isOffline: options.offline,
        maxNumberOfDeals: options.maxDeals
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('status')
  .description('Check the status of a deal replication request')
  .argument('<id>', 'A unique id of the dataset')
  .action(async (id, options) => {
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.get(`${url}/replication/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('list')
  .description('List all deal replication requests')
  .action(async (options) => {
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.get(`${url}/replications`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

program.showSuggestionAfterError();
program.showHelpAfterError('(add --help for additional information)');
program.parse();
