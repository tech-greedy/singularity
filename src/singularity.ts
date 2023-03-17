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
import DealTrackingService from './deal-tracking/DealTrackingService';
import GetPreparationDetailsResponse from './deal-preparation/model/GetPreparationDetailsResponse';
import fs from 'fs-extra';
import Logger, { Category } from './common/Logger';
import { Worker } from 'cluster';
import cron from 'node-cron';
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
import GenerateCsv from './common/GenerateCsv';
import { randomUUID } from 'crypto';
import MetricEmitter from './common/metrics/MetricEmitter';
import boxen from 'boxen';
import wrap from 'word-wrap';
import DealSelfService from './replication/selfservice/DealSelfService';

const version = packageJson.version;
const dataCollectionText = boxen(
  wrap(`By default, the usage of this software will be collected by the Slingshot Program to help improve the software. You can opt out by setting metrics.enabled to false in the config file ${path.join(getConfigDir(), 'default.toml')}.`,
    { width: 60, indent: '', trim: true }),
  { title: 'Usage Collection', padding: 1, margin: 1, borderStyle: 'double' });
let dataCollectionTextDisplayed = false;

async function migrate (): Promise<void> {
  const instanceFound = await Datastore.MiscModel.findOne({ key: 'instance' });
  if (!instanceFound) {
    if (!dataCollectionTextDisplayed) {
      console.info(dataCollectionText);
      dataCollectionTextDisplayed = true;
    }
    const configDir = getConfigDir();
    if (await fs.pathExists(path.join(configDir, 'instance.txt'))) {
      console.info('Migrating from v2.0.0-RC1 ... This may take a while.');
      const instanceId = await fs.readFile(path.join(configDir, 'instance.txt'), 'utf8');
      const stat = await fs.stat(path.join(configDir, 'instance.txt'));
      const until = stat.mtime;
      await Datastore.MiscModel.create({ key: 'instance', value: instanceId });
      await Datastore.MiscModel.create({ key: 'migrate_until', value: until });
      await Datastore.MiscModel.create({ key: 'migrated', value: false });
      await fs.remove(path.join(configDir, 'instance.txt'));
    } else {
      console.info('Migrating from v1.x ... This may take a while.');
      const instanceId = randomUUID();
      const until = Date.now();
      await Datastore.MiscModel.create({ key: 'instance', value: instanceId });
      await Datastore.MiscModel.create({ key: 'migrate_until', value: until });
      await Datastore.MiscModel.create({ key: 'migrated', value: false });
    }
  }

  ConfigInitializer.instanceId = (await Datastore.MiscModel.findOne({ key: 'instance' }))!.value;
  const migrated: boolean = (await Datastore.MiscModel.findOne({ key: 'migrated' }))!.value;
  const until: Date = (await Datastore.MiscModel.findOne({ key: 'migrate_until' }))!.value;

  if (!migrated && config.getOrDefault('metrics.enabled', true)) {
    MetricEmitter.Instance();
    console.info('Migrating completed generations from previous version ...');
    for (const generation of await Datastore.GenerationRequestModel.find({
      status: 'completed',
      updatedAt: { $lt: until }
    })) {
      let numOfFiles = 0;
      for (const fileList of await Datastore.OutputFileListModel.aggregate([{ $match: { generationId: generation.id } }, {
        $project: {
          numOfFiles: { $size: '$generatedFileList' }
        }
      }])) {
        numOfFiles += fileList.numOfFiles;
      }
      await MetricEmitter.Instance().emit({
        type: 'generation_complete',
        values: {
          datasetId: generation.datasetId,
          datasetName: generation.datasetName,
          generationId: generation.id,
          index: generation.index,
          dataCid: generation.dataCid,
          pieceSize: generation.pieceSize,
          pieceCid: generation.pieceCid,
          carSize: generation.carSize,
          numOfFiles: numOfFiles
        }
      }, generation.updatedAt);
    }

    console.info('Migrating deals from previous version ...');
    for (const deal of await Datastore.DealStateModel.find({ replicationRequestId: { $exists: true }, updatedAt: { $lt: until } })) {
      await MetricEmitter.Instance().emit({
        type: 'deal_proposed',
        values: {
          protocol: 'unknown',
          pieceCid: deal.pieceCid,
          dataCid: deal.dataCid,
          pieceSize: deal.pieceSize,
          carSize: deal.pieceSize,
          provider: deal.provider,
          client: deal.client,
          verified: deal.verified,
          duration: deal.duration,
          price: deal.price
        }
      }, deal.updatedAt);

      if (deal.state === 'active') {
        await MetricEmitter.Instance().emit({
          type: 'deal_active',
          values: {
            pieceCid: deal.pieceCid,
            pieceSize: deal.pieceSize,
            dataCid: deal.dataCid,
            provider: deal.provider,
            client: deal.client,
            verified: deal.verified,
            duration: deal.duration,
            price: deal.price
          }
        }, deal.updatedAt);
      }
    }

    console.info('Migration completed ...');
    await Datastore.MiscModel.findOneAndUpdate({ key: 'migrated' }, { value: true });
  }
}

async function initializeConfig (copyDefaultConfig: boolean, watchFile = false): Promise<void> {
  const configDir = getConfigDir();
  if (!await fs.pathExists(path.join(configDir, 'default.toml')) && copyDefaultConfig) {
    console.info(`Initializing at ${configDir} ...`);
    await fs.mkdirp(configDir);
    await fs.copyFile(path.join(__dirname, '../config/default.toml'), path.join(configDir, 'default.toml'));
    console.info(`Please check ${path.join(configDir, 'default.toml')}`);
    if (!dataCollectionTextDisplayed) {
      console.info(dataCollectionText);
      dataCollectionTextDisplayed = true;
    }
  }
  await ConfigInitializer.initialize();
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
      process.on('uncaughtException', (err, origin) => {
        console.error(err);
        console.error(origin);
        if (!err.message.includes('EPIPE') && !err.message.includes('Process exited')) {
          process.exit(1);
        }
      });
      if (cluster.isPrimary) {
        await Datastore.init(false);
        await Datastore.connect();
        await migrate();
        ConfigInitializer.instanceId = (await Datastore.MiscModel.findOne({ key: 'instance' }))!.value;
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
          await Datastore.HealthCheckModel.deleteMany();
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
        if (config.get('deal_tracking_service.enabled')) {
          workers.push([cluster.fork(), 'deal_tracking_service']);
        }
        if (config.get('deal_replication_service.enabled')) {
          workers.push([cluster.fork(), 'deal_replication_service']);
        }
        if (config.get('deal_replication_worker.enabled')) {
          workers.push([cluster.fork(), 'deal_replication_worker']);
        }
        if (config.getOrDefault('deal_self_service.enabled', false)) {
          workers.push([cluster.fork(), 'deal_self_service']);
        }
      } else if (cluster.isWorker) {
        await Datastore.connect();
        ConfigInitializer.instanceId = (await Datastore.MiscModel.findOne({ key: 'instance' }))!.value;
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
            case 'deal_self_service':
              new DealSelfService().start();
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
  .addOption(new Option('-l, --max-links <maxLinks>', 'Maximum number of links in each layer for a dynamic array or map').default(1000).argParser(Number))
  .addOption(new Option('-n, --max-nodes <maxNodes>', 'The threshold above which should the structure be linked rather than embedded').default(100).argParser(Number))
  .option('-m, --maxLink', 'The maximum number of links in each layer for a dynamic array or map', '1000')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.index_service');
    let response!: AxiosResponse;
    console.log(`Using maxLinks: ${options.maxLinks}, maxNodes: ${options.maxNodes}`);
    console.log(`Creating index for ${id} ...`);
    try {
      response = await axios.post(`${url}/create/${id}`, {
        maxLinks: options.maxLinks,
        maxNodes: options.maxNodes
      });
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
  .option('--force', 'Skip making client side check of whether dataset path exists.')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').argParser(parseFloat))
  .action(async (name, p: string, outDir, options) => {
    await initializeConfig(false, false);
    if (!options.force && !p.startsWith('s3://') && !await fs.pathExists(p)) {
      console.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    if (p.startsWith('s3://') && !options.tmpDir) {
      console.error('tmp_dir needs to specified for S3 dataset');
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

preparation.command('generate-dag-car').alias('dag')
  .description('Add a CAR file to the dataset that represents the Unixfs folder structure for the dataset. ' +
    'This will allow bitswap retrieval of the dataset. ' +
    'Be cautious to use it when the preparation is not done as it may lead to missing files in the final Root CID.')
  .argument('<dataset>', 'The dataset id or name')
  .action(async (id) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    console.log('Generating DAG CAR file. Please wait until it finishes.');
    try {
      response = await axios.post(`${url}/preparation/${id}/generate-dag`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, false);
    console.log('Once the DAG CAR file is sealed or made available on the network, you may retrieve the dataset using unixfs path with the Root CID:');
    console.log(response.data.dataCid);
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
  .addOption(new Option('-j, --concurrency <concurrency>', 'Number of concurrent uploads').default(2).argParser(Number))
  .action(async (dataset, slugName, options) => {
    await initializeConfig(false, false);
    const mongoose = await Datastore.connect();
    if (!process.env.WEB3_STORAGE_TOKEN) {
      console.error('WEB3_STORAGE_TOKEN is not set');
      process.exit(1);
    }
    const found = await Datastore.findScanningRequest(dataset);
    if (!found) {
      console.error(`Dataset ${dataset} not found`);
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
          },
          maxBodyLength: Infinity,
          maxContentLength: Infinity
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
      console.warn(`${incomplete} generation requests are not completed`);
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

preparation.command('append').description('Append a new directory to an existing dataset. ' +
  'This will add all entries under the new directory into the dataset. ' +
  'Just like the "singularity prep create" command, the directory will be considered as the root.\n' +
  'User is responsible of making sure there are no duplicate entries in the dataset otherwise the file with same path may be corrupted during retrieval.')
  .option('--json', 'Output with JSON format')
  .option('--force', 'Skip making client side check of whether dataset path exists.')
  .argument('<dataset>', 'The dataset id or name')
  .argument('<newPath>', 'Entries from that directory will be appended to the dataset')
  .action(async (dataset, p: string, options) => {
    await initializeConfig(false, false);
    if (!options.force && !p.startsWith('s3://') && !await fs.pathExists(p)) {
      console.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    const url: string = config.get('connection.deal_preparation_service');
    let response!: AxiosResponse;
    try {
      response = await axios.post(`${url}/preparation/${dataset}/append`, {
        path: p.startsWith('s3://') ? p : path.resolve(p)
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse(response.data, options.json);
  });

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

const selfservice = replication.command('selfservice')
  .alias('ss')
  .description('Manage deal making self service policies');

selfservice.command('list')
  .description('List all deal making self service policies')
  .option('--json', 'Output with JSON format')
  .action(async (options) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_self_service');
    try {
      const response = await axios.get(`${url}/policy`);
      CliUtil.renderResponse(response.data, options.json);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
  });

selfservice.command('delete')
  .description('Delete a deal making self service policy')
  .argument('<id>', 'Policy id to delete')
  .action(async (id) => {
    await initializeConfig(false, false);
    const url: string = config.get('connection.deal_self_service');
    try {
      await axios.delete(`${url}/policy/${id}`);
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
  });
selfservice.command('create')
  .description('Create a deal making self service policy')
  .argument('<client>', 'Client address to send deals from')
  .argument('<provider>', 'Provider address to send deals to')
  .argument('<dataset>', 'Id or name of the dataset')
  .option('--minDelay <minDelay>', 'Minimum delay in days for the deal start epoch', '7')
  .option('--maxDelay <maxDelay>', 'Maximum delay in days for the deal start epoch', '7')
  .option('-r, --verified <verified>', 'Whether to propose deal as verified. true|false.', 'true')
  .option('-p, --price <price>', 'Maximum price per epoch per GiB in Fil.', '0')
  .option('--minDuration <minDuration>', 'Minimum duration in days for the deal', '525')
  .option('--maxDuration <maxDuration>', 'maxDuration duration in days for the deal', '525')
  .action(async (client, provider, dataset, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    const url: string = config.get('connection.deal_self_service');
    try {
      response = await axios.post(`${url}/policy`, {
        client,
        provider,
        dataset,
        minStartDays: Number(options.minDelay),
        maxStartDays: Number(options.maxDelay),
        verified: options.verified === 'true',
        price: Number(options.price),
        minDurationDays: Number(options.minDuration),
        maxDurationDays: Number(options.maxDuration)
      });
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }

    CliUtil.renderResponse(response.data, false);
  });

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
  .option('-csv, --output-csv <outputCsv>', 'Print CSV to specified folder after done. Folder must exist on worker.')
  .option('-f, --force', 'Force resend even if this pieceCID have been proposed / active by the provider.', false)
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
        notes: options.notes ? options.notes : undefined,
        csvOutputDir: options.outputCsv ? options.outputCsv : undefined,
        isForced: options.force
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
  .option('-v, --verbose', 'Also print deal counts in this request', false)
  .action(async (options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.get(`${url}/replications?verbose=${options.verbose}`);
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

replication.command('complete').description('Forcefully mark an active deal replication request as completed.')
  .option('--json', 'Output with JSON format')
  .argument('<id>', 'Existing ID of deal replication request.')
  .action(async (id, options) => {
    await initializeConfig(false, false);
    let response!: AxiosResponse;
    try {
      const url: string = config.get('connection.deal_replication_service');
      response = await axios.post(`${url}/replication/${id}`, {
        status: 'completed'
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
      await initializeConfig(false, false);
      const mongoose = await Datastore.connect();
      msg = await GenerateCsv.generate(id, outDir);
      await mongoose.disconnect();
    } catch (error) {
      CliUtil.renderErrorAndExit(error);
    }
    CliUtil.renderResponse({ msg }, false);
  });

program.showSuggestionAfterError();
program.showHelpAfterError('(add --help for additional information)');
program.parse();
