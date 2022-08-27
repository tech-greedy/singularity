import ImportOptions from './ImportOptions';
import fs from 'fs-extra';
import path from 'path';
import JsonRpcClient from './JsonRpcClient';
import MinerDeal from './MinerDeal';
import Semaphore from 'semaphore-async-await';
import { sleep } from '../common/Util';
import MultipartDownloader from 'multipart-download';

function throwError (message?: any, ...optionalParams: any[]) {
  console.error(message, ...optionalParams);
  process.exit(1);
}

async function validateImportOptions (options: ImportOptions): Promise<JsonRpcClient> {
  if (!process.env.LOTUS_MINER_PATH ||
    !process.env.LOTUS_MARKETS_PATH ||
    !process.env.MINER_API_INFO ||
    !process.env.MARKETS_API_INFO) {
    throwError('Make sure you have one of the following environment variables set: LOTUS_MINER_PATH, LOTUS_MARKETS_PATH, MINER_API_INFO, MARKETS_API_INFO');
  }
  if (options.since <= 0) {
    throwError('--since must be greater than 0');
  }
  if (!options.path && !options.urlTemplate) {
    throwError('Either --path or --url-template is required');
  }
  if (options.urlTemplate && !options.downloadFolder) {
    throwError('--download-folder is required when --url-template is used');
  }
  if (options.interval < 0) {
    throwError('--interval must be greater than or equal to 0');
  }
  if (options.intervalCap < 1) {
    throwError('--interval-cap must be greater than or equal to 1');
  }
  if (options.downloadConcurrency < 1) {
    throwError('--download-concurrency must be greater than or equal to 1');
  }
  if (options.importConcurrency < 1) {
    throwError('--importConcurrency must be greater than or equal to 1');
  }
  if (options.importConcurrency > 1) {
    console.warn('The import concurrency is greater than 1.' +
      ' Make sure you have enough system resources to import multiple deals concurrently.');
  }
  if (options.loop && options.dryRun) {
    console.warn('The --loop option will be ignored when running as dry run.');
  }

  let token, addr: string;
  const minerPath = process.env.LOTUS_MARKETS_PATH || process.env.LOTUS_MINER_PATH;
  if (minerPath) {
    addr = await fs.readFile(path.join(minerPath, 'api'), 'utf8');
    token = await fs.readFile(path.join(minerPath, 'token'), 'utf8');
  } else {
    const minerApiInfo = process.env.MARKETS_API_INFO || process.env.MINER_API_INFO;
    [token, addr] = minerApiInfo!.split(':');
  }
  let ip = addr.split('/')[2];
  if (ip === '0.0.0.0') {
    ip = '127.0.0.1';
  }
  const port = addr.split('/')[4];
  return new JsonRpcClient(`http://${ip}:${port}/rpc/v0`, 'Filecoin.', {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });
}

export async function startImportLoop (options: ImportOptions) {
  const client = await validateImportOptions(options);
  if (options.loop && !options.dryRun) {
    await startImport(options, client);
  }
}

async function downloadFile (url: string, dest: string, threads: number) {
  return new Promise((resolve, reject) => {
    const downloader = new MultipartDownloader();
    downloader.start(url, {
      fileName: path.basename(dest),
      saveDirectory: path.dirname(dest),
      numOfConnections: threads
    });
    downloader.on('error', reject);
    downloader.on('end', resolve);
  });
}

async function download (url: string, dest: string, options: ImportOptions, downloadSemaphore: Semaphore): Promise<boolean> {
  try {
    await fs.ensureDir(options.downloadFolder!);
    console.log(`Downloading ${url} to ${dest}`);
    if (!options.dryRun) {
      await downloadSemaphore.acquire();
      await downloadFile(url, dest + '.download', options.downloadThreads);
      await fs.rename(dest + '.download', dest);
    }
  } catch (e) {
    console.error(e);
    return false;
  } finally {
    downloadSemaphore.release();
  }
  return true;
}

async function importDeal (
  existingPath: string,
  deal: MinerDeal,
  client: JsonRpcClient,
  options: ImportOptions,
  importSemaphore: Semaphore) {
  console.log(`Importing ${existingPath} with Proposal ${deal.ProposalCid['/']}`);
  if (!options.dryRun) {
    try {
      await importSemaphore.acquire();
      await client.call('DealsImportData', [deal.ProposalCid, path.resolve(existingPath)]);
      if (options.removeImported) {
        await fs.rm(existingPath);
      }
    } catch (e) {
      console.error(e);
      throw e;
    } finally {
      importSemaphore.release();
    }
  }
}

async function startImport (options: ImportOptions, client: JsonRpcClient) {
  console.log('Fetching deals from Miner...');
  const response = await client.call('MarketListIncompleteDeals', []);
  if (response.error) {
    throwError(JSON.stringify(response.error));
  }
  const deals = <MinerDeal[]>response.result;
  const importSemaphore = new Semaphore(options.importConcurrency);
  const downloadSemaphore = new Semaphore(options.importConcurrency);
  for (let i = 0; i < deals.length; ++i) {
    const deal = deals[i];
    if (options.client && !options.client.includes(deal.Proposal.Client)) {
      continue;
    }
    if (deal.State !== 18) {
      continue;
    }
    if (Date.now() - Date.parse(deal.CreationTime) > options.since * 1000) {
      continue;
    }
    let existingPath: string | undefined;
    // Check if the file already exists in --path.
    if (options.path) {
      for (const p of options.path) {
        const dataCidFile = path.join(p, deal.Refs.Root['/'] + '.car');
        const pieceCidFile = path.join(p, deal.Refs.PieceCid['/'] + '.car');
        if (await fs.pathExists(dataCidFile)) {
          existingPath = dataCidFile;
          break;
        }
        if (await fs.pathExists(pieceCidFile)) {
          existingPath = pieceCidFile;
          break;
        }
      }
    }
    if (existingPath) {
      if (i > 0) {
        await sleep(options.interval * 1000);
      }
      importDeal(existingPath, deal, client, options, importSemaphore);
      continue;
    }
    if (!options.urlTemplate) {
      continue;
    }

    // Download the file to specified download folder
    const pieceCidFile = path.join(options.downloadFolder!, deal.Refs.Root['/'] + '.car');
    const url = options.urlTemplate
      .replace('{pieceCid}', deal.Refs.PieceCid['/'])
      .replace('{dataCid}', deal.Refs.Root['/']);
    if (i > 0) {
      await sleep(options.interval * 1000);
    }
    (async () => {
      const success = await download(url, pieceCidFile, options, downloadSemaphore);
      if (success) {
        await importDeal(pieceCidFile, deal, client, options, importSemaphore);
      }
    })();
  }
}
