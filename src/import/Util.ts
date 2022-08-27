import ImportOptions from './ImportOptions';
import fs from 'fs-extra';
import path from 'path';
import JsonRpcClient from './JsonRpcClient';
import MinerDeal from './MinerDeal';
import PQueue from 'p-queue';
import Semaphore from 'semaphore-async-await';

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
  if (!options.path && !options.urlTemplate) {
    throwError('Either --path or --url-template is required');
  }
  if (options.urlTemplate && !options.downloadFolder) {
    throwError('--download-folder is required when --url-template is used');
  }
  if (options.interval < 0) {
    throwError('--interval must be greater than or equal to 0');
  }
  if (options.concurrency < 1) {
    throwError('--concurrency must be greater than or equal to 1');
  }
  if (options.importConcurrency < 1) {
    throwError('--importConcurrency must be greater than or equal to 1');
  }
  if (options.interval < 120 && options.importConcurrency > 1) {
    console.warn('The interval is less than 120s and the concurrency imports are greater than 1.' +
      ' This may lead to OOM. Make sure you understand what you are doing.');
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

async function startImport (options: ImportOptions, client: JsonRpcClient) {
  console.log('Fetching deals from Miner...');
  const response = await client.call('MarketListIncompleteDeals', []);
  if (response.error) {
    throwError(JSON.stringify(response.error));
  }
  const deals = <MinerDeal[]>response.result;
  const queue = new PQueue({
    concurrency: options.concurrency,
    interval: options.interval * 1000,
    intervalCap: 1
  });
  const sem = new Semaphore(options.importConcurrency);
  for (const deal of deals) {
    if (options.client && !options.client.includes(deal.Proposal.Client)) {
      continue;
    }
    queue.add(async () => {
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
      if (!options.urlTemplate) {
        return;
      }
      if (!existingPath) {
        // Download the file to specified download folder
        const pieceCidFile = path.join(options.downloadFolder!, deal.Refs.Root['/'] + '.car');
        const url = options.urlTemplate
          .replace('{pieceCid}', deal.Refs.PieceCid['/'])
          .replace('{dataCid}', deal.Refs.Root['/']);
        try {
          await fs.ensureDir(options.downloadFolder!);
          console.log(`Downloading ${url} to ${pieceCidFile}`);
          if (!options.dryRun) {
            await download(url, pieceCidFile);
          }
          existingPath = pieceCidFile;
        } catch (e) {
          console.error(e);
        }
      }
      if (existingPath) {
        console.log(`Importing ${existingPath} with Proposal ${deal.ProposalCid['/']}`);
        if (!options.dryRun) {
          try {
            await sem.acquire();
            await client.call('DealsImportData', [deal.ProposalCid, path.resolve(existingPath)]);
          } catch (e) {
            console.error(e);
            throw e;
          } finally {
            sem.release();
            if (options.removeImported) {
              await fs.rm(existingPath);
            }
          }
        }
      }
    });
  }
}
