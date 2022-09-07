#!/usr/bin/env node
/* eslint-disable import/first */
/* eslint @typescript-eslint/no-var-requires: "off" */
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
import CliUtil from './cli-util';
import IndexService from './index/IndexService';
import DealTrackingService from './deal-tracking/DealTrackingService';
import GetPreparationDetailsResponse from './deal-preparation/model/GetPreparationDetailsResponse';
import fs from 'fs-extra';
import Logger, { Category } from './common/Logger';
import { Worker } from 'cluster';
import cron from 'node-cron';
import * as IpfsCore from 'ipfs-core';
import DealReplicationService from './replication/DealReplicationService';
import DealReplicationWorker from './replication/DealReplicationWorker';
import GenerateCar from './common/GenerateCar';
import HealthCheck from './common/model/HealthCheck';
import xbytes from 'xbytes';
import config, { ConfigInitializer, getConfigDir } from './common/Config';
import { getContentsAndGroupings } from './deal-preparation/handler/GetGenerationManifestRequestHandler';
import canonicalize from 'canonicalize';
import { compress } from '@xingrz/cppzst';
import progress from 'cli-progress';
import asyncRetry from 'async-retry';
import pAll from 'p-all';
import ObjectsToCsv from 'objects-to-csv';

const logger = Logger.getLogger(Category.Default);
const version = packageJson.version;

async function initializeConfig (copyDefaultConfig: boolean, watchFile = false): Promise<void> {
  const configDir = getConfigDir();
  if (!await fs.pathExists(path.join(configDir, 'default.toml')) && copyDefaultConfig) {
    logger.info(`Initializing at ${configDir} ...`);
    await fs.mkdirp(configDir);
    await fs.copyFile(path.join(__dirname, '../config/default.toml'), path.join(configDir, 'default.toml'));
    logger.info(`Please check ${path.join(configDir, 'default.toml')}`);
  }
  ConfigInitializer.initialize();
  if (watchFile) {
    ConfigInitializer.watchFile();
  }
}

const program = new Command();
program.name('singularity')
  .version(version)
  .description('A tool for large-scale clients with PB-scale data onboarding to Filecoin network\nVisit https://github.com/tech-greedy/singularity for more details');

program.command('init')
  .description('Initialize the configuration directory in SINGULARITY_PATH\nIf unset, it will be initialized at HOME_DIR/.singularity')
  .action(async () => {
    await initializeConfig(true, false);
  });

program.command('daemon')
  .description('Start a daemon process for deal preparation and deal making')
  .action((_options) => {
    (async function () {
      await initializeConfig(true, true);
      GenerateCar.initialize();
      if (cluster.isPrimary) {
        let indexService: IndexService;
        await Datastore.init(false);
        await Datastore.connect();
        await Datastore.HealthCheckModel.deleteMany();
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
          if (config.get('deal_preparation_service.enable_cleanup')) {
            await DealPreparationService.cleanupIncompleteFiles(Logger.getLogger(Category.Default));
          }
          workers.push([cluster.fork(), 'deal_preparation_service']);
        }
        if (config.get('deal_preparation_worker.enabled')) {
          const numWorkers = config.has('deal_preparation_worker.num_workers') ? config.get<number>('deal_preparation_worker.num_workers') : 1;
          for (let i = 0; i < numWorkers; ++i) {
            workers.push([cluster.fork(), 'deal_preparation_worker']);
          }
        }
        if (config.get('index_service.enabled')) {
          indexService = new IndexService();
          if (config.get('ipfs.enabled')) {
            indexService['ipfsClient'] = await IpfsCore.create();
          }
          indexService.start();
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
    await initializeConfig(false, false);
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

function sleep (ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

program.command('monitor').description('Monitor worker status and download speed')
  .action(async () => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    while (true) {
      try {
        response = await axios.get(`${url}/monitor`);
      } catch (error) {
        CliUtil.renderErrorAndExit(error);
      }

      const data: HealthCheck[] = response.data;
      const result: { [key: string]: any } = {};
      for (const d of data) {
        result[d.pid.toString()] = {
          ...d,
          downloadSpeed: xbytes(d.downloadSpeed) + '/s',
          cpuUsage: (d.cpuUsage ?? 0).toFixed(2) + '%',
          memoryUsage: xbytes(d.memoryUsage ?? 0),
          childCpuUsage: (d.childCpuUsage ?? 0).toFixed(2) + '%',
          childMemoryUsage: xbytes(d.childMemoryUsage ?? 0)
        };
      }
      result['Total'] = {
        downloadSpeed:
          xbytes(data.reduce((acc, d) => acc + d.downloadSpeed, 0)) + '/s',
        cpuUsage:
          (data.reduce((acc, d) => acc + (d.cpuUsage ?? 0), 0)).toFixed(2) + '%',
        memoryUsage:
          xbytes(data.reduce((acc, d) => acc + (d.memoryUsage ?? 0), 0)),
        childCpuUsage:
          (data.reduce((acc, d) => acc + (d.childCpuUsage ?? 0), 0)).toFixed(2) + '%',
        childMemoryUsage:
          xbytes(data.reduce((acc, d) => acc + (d.childMemoryUsage ?? 0), 0))
      };
      CliUtil.renderResponse(result, false);
      await sleep(5000);
    }
  });

preparation.command('create').description('Start deal preparation for a local dataset')
  .argument('<datasetName>', 'A unique name of the dataset')
  .argument('<datasetPath>', 'Directory path to the dataset')
  .argument('<outDir>', 'The output Directory to save CAR files')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 32GiB', '32 GiB')
  .option('-t, --tmp-dir <tmp_dir>', 'Optional temporary directory. May be useful when it is at least 2x faster than the dataset source, such as when the dataset is on network mount, and the I/O is the bottleneck')
  .option('-f, --skip-inaccessible-files', 'Skip inaccessible files. Scanning may take longer to complete.')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').argParser(parseFloat))
  .action(async (name, p: string, outDir, options) => {
    await initializeConfig(false, false);
    if (!p.startsWith('s3://') && !await fs.pathExists(p)) {
      logger.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    if (p.startsWith('s3://') && !options.tmpDir) {
      logger.error('tmp_dir needs to specified for S3 dataset');
      process.exit(1);
    }
    await fs.mkdirp(outDir);
    if (options.tmpDir) {
      await fs.mkdirp(options.tmpDir);
    }
    const dealSize: string = options.dealSize;
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = await axios.post(`${url}/preparation`, {
        name: name,
        path: p.startsWith('s3://') ? p : path.resolve(p),
        dealSize: dealSize,
        outDir: path.resolve(outDir),
        minRatio: options.minRatio,
        maxRatio: options.maxRatio,
        tmpDir: options.tmpDir ? path.resolve(options.tmpDir) : undefined,
        skipInaccessibleFiles: options.skipInaccessibleFiles
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
    await initializeConfig(false, false);
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
      const {
        generationRequests,
        ...summary
      } = data;
      console.log('Scanning Request Summary');
      console.table([summary]);
      console.log('Corresponding Generation Requests');
      console.table(generationRequests);
    }
  });

preparation.command('list').description('List all deal preparation requests')
  .option('--json', 'Output with JSON format')
  .action(async (options) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = await axios.get(`${url}/preparations`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('upload-manifest').description('Upload manifest to web3.storage')
  .argument('<dataset>', 'The dataset id or name, as in "singularity prep list"')
  .argument('<slugName>', 'The slug name of the dataset, as shown on "My Claimed Datasets" page')
  .addOption(new Option('-j, --concurrency <concurrency>', 'Number of concurrent uploads').default(2).argParser(parseInt))
  .action(async (dataset, slugName, options) => {
    await initializeConfig(false, false);
    const mongoose = await Datastore.connect();
    if (!process.env.WEB3_STORAGE_TOKEN) {
      logger.error('WEB3_STORAGE_TOKEN is not set');
      process.exit(1);
    }
    const found = await Datastore.findScanningRequest(dataset);
    if (!found) {
      logger.error(`Dataset ${dataset} not found`);
      process.exit(1);
    }

    const generationRequests = await Datastore.GenerationRequestModel.find({ datasetId: found.id });
    const bar = new progress.SingleBar({}, progress.Presets.shades_classic);
    bar.start(generationRequests.length, 0);
    let incomplete = 0;
    const jobs = generationRequests.map(generationRequest => async () => {
      if (generationRequest.status !== 'completed') {
        incomplete++;
        bar.increment();
        return;
      }
      const uploadState = await Datastore.ManifestUploadStateModel.findOne(
        {
          state: 'complete',
          pieceCid: generationRequest.pieceCid,
          slugName: slugName,
          datasetId: found.id
        });
      if (uploadState) {
        bar.increment();
        return;
      }
      const generatedFileList = (await Datastore.OutputFileListModel.find({
        datasetId: generationRequest.id
      })).map(r => r.generatedFileList).flat();
      const [contents, groupings] = getContentsAndGroupings(generatedFileList);
      const result = {
        piece_cid: generationRequest.pieceCid,
        payload_cid: generationRequest.dataCid,
        raw_car_file_size: generationRequest.carSize,
        dataset: slugName,
        contents,
        groupings
      };
      const json = canonicalize(result);
      const compressed = await compress(Buffer.from(json!, 'utf8'));
      await asyncRetry(async () => {
        await axios.post('https://api.web3.storage/upload', compressed, {
          headers: {
            Authorization: `Bearer ${process.env.WEB3_STORAGE_TOKEN}`,
            'X-NAME': `${result.piece_cid}.json.zst`
          }
        });
      }, {
        retries: 5,
        minTimeout: 2500
      });
      await Datastore.ManifestUploadStateModel.create({
        pieceCid: result.piece_cid,
        slugName: slugName,
        datasetId: found.id,
        state: 'complete'
      });
      bar.increment();
    });
    await pAll(jobs, {
      stopOnError: true,
      concurrency: options.concurrency
    });
    await mongoose.disconnect();
    bar.stop();
    if (incomplete > 0) {
      logger.warn(`${incomplete} generation requests are not completed`);
    }
  });

preparation.command('update-generation').description('Update generation request')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generationId>', 'The id or index for the generation request').argOptional())
  .option('-t, --tmp-dir <tmp_dir>', 'Change the temporary directory')
  .option('-o, --out-dir <out_dir>', 'Change the output directory')
  .option('-f, --skip-inaccessible-files', 'Change whether to skip inaccessible files')
  .action(async (dataset, generationId, options) => {
    await initializeConfig(false, false);
    if (options.tmpDir) {
      await fs.mkdirp(options.tmpDir);
      options.tmpDir = path.resolve(options.tmpDir);
    }
    if (options.outDir) {
      await fs.mkdirp(options.outDir);
      options.outDir = path.resolve(options.outDir);
    }
    const response = await UpdateGenerationState(dataset, generationId, {
      tmpDir: options.tmpDir,
      outDir: options.outDir,
      skipInaccessibleFiles: options.skipInaccessibleFiles
    });
    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('generation-manifest').description('Get the Slingshot v3.x manifest data for a single deal generation request')
  .option('--dataset <dataset>', 'The dataset id or name, required if looking for generation request using index')
  .option('--pretty', 'Whether to add indents to output JSON')
  .option('--name-override <name_override>', 'Override the dataset name in the output JSON. This is the slug name in Slingshot V3.')
  .argument('<generationId>', 'A unique id or index of the generation request')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = options.dataset ? await axios.get(`${url}/generation-manifest/${options.dataset}/${id}`) : await axios.get(`${url}/generation-manifest/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    const data = response.data;
    if (options.nameOverride) {
      data.dataset = options.nameOverride;
    }
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
    await initializeConfig(false, false);
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
      const {
        fileList,
        generatedFileList,
        ...summary
      } = data;
      console.log('Generation Request Summary');
      console.table([summary]);
      console.log('File Lists');
      console.table(fileList.length > 0 ? fileList : generatedFileList);
    }
  });

async function UpdateScanningState (id: string, action: string): Promise<AxiosResponse> {
  const url: string = config.get('connection.deal_preparation_service');
  let response!: AxiosResponse;
  try {
    response = await axios.post(`${url}/preparation/${id}`, { action });
  } catch (error) {
    CliUtil.renderErrorAndExit(error);
  }
  return response;
}

async function UpdateGenerationState (dataset: string, generation: string | undefined, update: any): Promise<AxiosResponse> {
  const url: string = config.get('connection.deal_preparation_service');
  let response!: AxiosResponse;
  try {
    if (generation) {
      response = await axios.post(`${url}/generation/${dataset}/${generation}`, update);
    } else {
      response = await axios.post(`${url}/generation/${dataset}`, update);
    }
  } catch (error) {
    CliUtil.renderErrorAndExit(error);
  }
  return response;
}

const pause = preparation.command('pause')
  .description('Pause scanning or generation requests');
const resume = preparation.command('resume')
  .description('Resume scanning or generation requests');
const retry = preparation.command('retry')
  .description('Retry scanning or generation requests');

pause.command('scanning').alias('scan').description('Pause an active data scanning request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    const response = await UpdateScanningState(id, 'pause');
    CliUtil.renderResponse(response.data, options.json);
  });

resume.command('scanning').alias('scan').description('Resume a paused data scanning request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    const response = await UpdateScanningState(id, 'resume');
    CliUtil.renderResponse(response.data, options.json);
  });

retry.command('scanning').alias('scan').description('Retry an errored data scanning request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    const response = await UpdateScanningState(id, 'retry');
    CliUtil.renderResponse(response.data, options.json);
  });

pause.command('generation').alias('gen').description('Pause an active data generation request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generation_id>', 'The id or index for the generation request').argOptional())
  .action(async (dataset, generation, options) => {
    await initializeConfig(false, false);
    const response = await UpdateGenerationState(dataset, generation, { action: 'pause' });
    CliUtil.renderResponse(response.data, options.json);
  });

resume.command('generation').alias('gen').description('Resume a paused data generation request')
  .option('--json', 'Output with JSON format')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generation_id>', 'The id or index for the generation request').argOptional())
  .action(async (dataset, generation, options) => {
    await initializeConfig(false, false);
    const response = await UpdateGenerationState(dataset, generation, { action: 'resume' });
    CliUtil.renderResponse(response.data, options.json);
  });

retry.command('generation').alias('gen').description('Retry an errored data generation request')
  .option('--json', 'Output with JSON format')
  .option('--force', 'Force retry the generation even if the generation has completed')
  .option('-f, --skip-inaccessible-files', 'Skip inaccessible files. This may lead to a smaller CAR file being generated.')
  .argument('<dataset>', 'The dataset id or name')
  .addArgument(new Argument('<generation_id>', 'The id or index for the generation request').argOptional())
  .action(async (dataset, generation, options) => {
    await initializeConfig(false, false);
    const action = options.force ? 'forceRetry' : 'retry';
    const update = {
      action,
      skipInaccessibleFiles: options.skipInaccessibleFiles
    };
    const response = await UpdateGenerationState(dataset, generation, update);
    CliUtil.renderResponse(response.data, options.json);
  });

preparation.command('remove').description('Remove all records from database for a dataset')
  .option('--purge', 'Whether to also purge the car files')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id, options) => {
    await initializeConfig(false, false);
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
  .argument('<storage-providers>', 'Comma separated storage provider list')
  .argument('<client>', 'Client address where deals are proposed from')
  .argument('[# of replica]', 'Number of targeting replica of the dataset', 10)
  .option('-u, --url-prefix <urlprefix>', 'URL prefix for car downloading. Must be reachable by provider\'s boostd node.', 'http://127.0.0.1/')
  .option('-p, --price <maxprice>', 'Maximum price per epoch per GiB in Fil.', '0')
  .option('-r, --verified <verified>', 'Whether to propose deal as verified. true|false.', 'true')
  .option('-s, --start-delay <startdelay>', 'Deal start delay in days. (StartEpoch)', '7')
  .option('-d, --duration <duration>', 'Duration in days for deal length.', '525')
  .option('-o, --offline <offline>', 'Propose as offline deal.', 'true')
  .option('-m, --max-deals <maxdeals>', 'Max number of deals in this replication request per SP, per cron triggered.', '0')
  .option('-c, --cron-schedule <cronschedule>', 'Optional cron to send deals at interval. Use double quote to wrap the format containing spaces.')
  .option('-x, --cron-max-deals <cronmaxdeals>', 'When cron schedule specified, limit the total number of deals across entire cron, per SP.')
  .option('-xp, --cron-max-pending-deals <cronmaxpendingdeals>', 'When cron schedule specified, limit the total number of pending deals determined by dealtracking service, per SP.')
  .option('-l, --file-list-path <filelistpath>', 'Absolute path to a txt file that will limit to replicate only from the list. Must be visible by deal replication worker.')
  .option('-n, --notes <notes>', 'Any notes or tag want to store along the replication request, for tracking purpose.')
  .action(async (datasetid, storageProviders, client, replica, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      console.log(datasetid, storageProviders, client, replica, options);
      if (options.cronSchedule) {
        if (!cron.validate(options.cronSchedule)) {
          CliUtil.renderErrorAndExit(`Invalid cron schedule format ${options.cronSchedule}. Try https://crontab.guru/ for a sample.`);
        }
      }
      if ((options.startDelay * 1 + options.duration * 1) > 540) {
        CliUtil.renderErrorAndExit(`Start Delay + Duration cannot exceed 540 days.`);
      }
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication`, {
        datasetId: datasetid,
        replica: replica,
        storageProviders: storageProviders,
        client: client,
        urlPrefix: options.urlPrefix,
        maxPrice: options.price,
        isVerfied: options.verified,
        startDelay: options.startDelay * 2880, // convert to epoch
        duration: options.duration * 2880, // convert to epoch
        isOffline: options.offline,
        maxNumberOfDeals: options.maxDeals,
        cronSchedule: options.cronSchedule ? options.cronSchedule : undefined,
        cronMaxDeals: options.cronMaxDeals ? options.cronMaxDeals : undefined,
        cronMaxPendingDeals: options.cronMaxPendingDeals ? options.cronMaxPendingDeals : undefined,
        fileListPath: options.fileListPath ? options.fileListPath : undefined,
        notes: options.notes ? options.notes : undefined
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('status')
  .description('Check the status of a deal replication request')
  .argument('<id>', 'A unique id of the dataset')
  .option('-v, --verbose', 'Also print list of deals in this request', false)
  .action(async (id, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.get(`${url}/replication/${id}?verbose=${options.verbose}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.verbose);
  });

replication.command('list')
  .description('List all deal replication requests')
  .action(async (options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.get(`${url}/replications`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('reschedule')
  .description('Change an existing deal replication request\'s cron schedule.')
  .argument('<id>', 'Existing ID of deal replication request.')
  .argument('<schedule>', 'Updated cron schedule.')
  .argument('<cronMaxDeals>', 'Updated max number of deals across entire cron schedule, per SP. Specify 0 for unlimited.')
  .argument('<cronMaxPendingDeals>', 'Updated max number of pending deals across entire cron schedule, per SP. Specify 0 for unlimited.')
  .action(async (id, schedule, cronMaxDeals, cronMaxPendingDeals, options) => {
    await initializeConfig(false, false);
    if (!cron.validate(schedule)) {
      CliUtil.renderErrorAndExit(`Invalid cron schedule format ${schedule}. Try https://crontab.guru/ for a sample.`);
    }

    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication/${id}`, {
        cronSchedule: schedule,
        cronMaxDeals: cronMaxDeals,
        cronMaxPendingDeals: cronMaxPendingDeals
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('pause').description('Pause an active deal replication request.')
  .option('--json', 'Output with JSON format')
  .argument('<id>', 'Existing ID of deal replication request.')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication/${id}`, {
        status: 'paused'
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('resume').description('Resume a paused deal replication request.')
  .option('--json', 'Output with JSON format')
  .argument('<id>', 'Existing ID of deal replication request.')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication/${id}`, {
        status: 'active'
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse(response.data, options.json);
  });

replication.command('csv').description('Write a deal replication result as csv.')
  .argument('<id>', 'Existing ID of deal replication request.')
  .argument('<outDir>', 'The output Directory to save the CSV file.')
  .action(async (id, outDir) => {
    let msg = '';
    try {
      fs.mkdirpSync(outDir);
      await initializeConfig(false, false);
      const mongoose = await Datastore.connect();
      const replicationRequest = await Datastore.ReplicationRequestModel.findById(id);
      if (replicationRequest) {
        const providers = DealReplicationWorker.generateProvidersList(replicationRequest.storageProviders);
        for (let j = 0; j < providers.length; j++) {
          const provider = providers[j];
          const deals = await Datastore.DealStateModel.find({
            replicationRequestId: id,
            provider: provider,
            state: { $nin: ['slashed', 'error', 'expired', 'proposal_expired'] }
          });
          let urlPrefix = replicationRequest.urlPrefix;
          if (!urlPrefix.endsWith('/')) {
            urlPrefix += '/';
          }

          if (deals.length > 0) {
            const csvRow = [];
            for (let i = 0; i < deals.length; i++) {
              const deal = deals[i];
              csvRow.push({
                miner_id: deal.provider,
                deal_cid: deal.dealCid,
                filename: `${deal.pieceCid}.car`,
                piece_cid: deal.pieceCid,
                start_epoch: deal.startEpoch,
                full_url: `${urlPrefix}${deal.pieceCid}.car`
              });
            }
            const csv = new ObjectsToCsv(csvRow);
            let fileListFilename = '';
            if (replicationRequest.fileListPath) {
              fileListFilename += '_' + path.parse(replicationRequest.fileListPath).name;
            }
            const filename = path.join(outDir, `${provider}${fileListFilename}_${id}.csv`);
            await csv.toDisk(filename);
            msg += `CSV saved to ${filename}\n`;
          } else {
            msg += `No deal found to export in ${id}\n`;
          }
        }
      } else {
        msg = `Replication request not found ${id}`;
      }
      await mongoose.disconnect();
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse({ msg }, false);
  });

program.showSuggestionAfterError();
program.showHelpAfterError('(add --help for additional information)');
program.parse();
