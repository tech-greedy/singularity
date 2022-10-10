import ImportOptions from './ImportOptions';
import fs from 'fs-extra';
import path from 'path';
import JsonRpcClient from './JsonRpcClient';
import MinerDeal, { Cid } from './MinerDeal';
import Semaphore from 'semaphore-async-await';
import { sleep } from '../common/Util';
import MultipartDownloader from 'multipart-download';
import { ErrorMessage } from './ErrorMessage';
import { AbortSignal } from '../common/AbortSignal';

export default class ImportUtil {
  public static throwError (message: string) {
    throw new Error(message);
  }

  public static knownBadProposalCids: string[] = [];

  private static async validateImportOptions (options: ImportOptions): Promise<JsonRpcClient> {
    console.log(options);
    if (!process.env.LOTUS_MINER_PATH &&
      !process.env.LOTUS_MARKETS_PATH &&
      !process.env.MINER_API_INFO &&
      !process.env.MARKETS_API_INFO) {
      ImportUtil.throwError(ErrorMessage.LOTUS_MINER_PATH_MISSING);
    }
    if (options.since <= 0) {
      ImportUtil.throwError(ErrorMessage.SINCE_LESS_THAN_0);
    }
    if (!options.path && !options.urlTemplate) {
      ImportUtil.throwError(ErrorMessage.PATH_OR_URL_TEMPLATE_REQUIRED);
    }
    if (options.urlTemplate && !options.downloadFolder) {
      ImportUtil.throwError(ErrorMessage.DOWNLOAD_FOLDER_REQUIRED);
    }
    if (options.interval < 0) {
      ImportUtil.throwError(ErrorMessage.INTERVAL_LESS_THAN_0);
    }
    if (options.intervalCap < 1) {
      ImportUtil.throwError(ErrorMessage.INTERVAL_CAP_LESS_THAN_1);
    }
    if (options.downloadConcurrency < 1) {
      ImportUtil.throwError(ErrorMessage.DOWNLOAD_CONCURRENCY_LESS_THAN_1);
    }
    if (options.importConcurrency < 1) {
      ImportUtil.throwError(ErrorMessage.IMPORT_CONCURRENCY_LESS_THAN_1);
    }
    if (options.importConcurrency > 1) {
      console.warn(ErrorMessage.IMPORT_CONCURRENCY_GREATER_THAN_1);
    }
    if (options.loop && options.dryRun) {
      console.warn(ErrorMessage.LOOP_AND_DRY_RUN);
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

  public static async startImportLoop (options: ImportOptions, abortSignal?: AbortSignal) {
    const client = await ImportUtil.validateImportOptions(options);
    if (!options.loop || options.dryRun) {
      await ImportUtil.startImport(options, client);
    } else {
      while (true) {
        await ImportUtil.startImport(options, client);
        if (abortSignal && await abortSignal()) {
          break;
        }
        await sleep(options.interval * 1000);
        if (abortSignal && await abortSignal()) {
          break;
        }
      }
    }
  }

  private static async downloadFile (url: string, dest: string, threads: number) {
    return new Promise((resolve, reject) => {
      const downloader = new MultipartDownloader();
      dest = path.resolve(dest);
      downloader.start(url, {
        fileName: path.basename(dest),
        saveDirectory: path.dirname(dest),
        numOfConnections: threads
      });
      downloader.on('error', reject);
      downloader.on('end', () => {
        resolve(undefined);
      });
    });
  }

  private static async download (url: string, dest: string, options: ImportOptions, downloadSemaphore: Semaphore): Promise<boolean> {
    try {
      console.log(`Downloading ${url} to ${dest}`);
      await downloadSemaphore.acquire();
      if (!options.dryRun) {
        await fs.ensureDir(options.downloadFolder!);
        await ImportUtil.downloadFile(url, dest + '.downloading', options.downloadThreads);
        await fs.rename(dest + '.downloading', dest);
      }
    } catch (e) {
      console.error(e);
      return false;
    } finally {
      downloadSemaphore.release();
    }
    return true;
  }

  private static async importDeal (
    existingPath: string,
    proposalCid: Cid,
    client: JsonRpcClient,
    options: ImportOptions,
    importSemaphore: Semaphore) {
    console.log(`Importing ${existingPath} with Proposal ${proposalCid['/']}`);
    try {
      await importSemaphore.acquire();
      if (!options.dryRun) {
        const response = await client.call('DealsImportData', [proposalCid, path.resolve(existingPath)]);
        if (response.error) {
          throw response.error;
        }
        if (options.removeImported) {
          await fs.rm(existingPath);
        }
      }
    } catch (e) {
      console.error(e);
      ImportUtil.knownBadProposalCids.push(proposalCid['/']);
      console.log(`Will no longer handle this proposal: ${proposalCid['/']}`);
    } finally {
      importSemaphore.release();
    }
  }

  private static async startImport (options: ImportOptions, client: JsonRpcClient) {
    console.log('Fetching deals from Miner...');
    const response = await client.call('MarketListIncompleteDeals', []);
    if (response.error) {
      ImportUtil.throwError(JSON.stringify(response.error));
    }
    const deals = <MinerDeal[]>response.result;
    const importSemaphore = new Semaphore(options.importConcurrency);
    const downloadSemaphore = new Semaphore(options.importConcurrency);
    let started = false;
    for (const deal of deals) {
      if (options.client && !options.client.includes(deal.Proposal.Client)) {
        continue;
      }
      if (deal.State !== 18) {
        continue;
      }
      if (Date.now() - Date.parse(deal.CreationTime) > options.since * 1000) {
        continue;
      }
      if (ImportUtil.knownBadProposalCids.includes(deal.ProposalCid['/'])) {
        continue;
      }
      let existingPath: string | undefined;
      // Check if the file already exists in --path.
      if (options.path) {
        for (const p of options.path) {
          const dataCidFile = path.join(p, deal.Ref.Root['/'] + '.car');
          const pieceCidFile = path.join(p, deal.Ref.PieceCid['/'] + '.car');
          if (await fs.pathExists(pieceCidFile)) {
            existingPath = pieceCidFile;
            break;
          }
          if (await fs.pathExists(dataCidFile)) {
            existingPath = dataCidFile;
            break;
          }
        }
      }
      if (existingPath) {
        if (started) {
          do {
            await sleep(options.interval * 1000);
          } while (options.downloadConcurrency + options.importConcurrency -
          importSemaphore.getPermits() - downloadSemaphore.getPermits() > options.intervalCap);
        }
        started = true;
        ImportUtil.importDeal(existingPath, deal.ProposalCid, client, options, importSemaphore);
        continue;
      }
      if (!options.urlTemplate) {
        continue;
      }

      // Download the file to specified download folder
      const pieceCidFile = path.join(options.downloadFolder!, deal.Ref.Root['/'] + '.car');
      const url = options.urlTemplate
        .replace('{pieceCid}', deal.Ref.PieceCid['/'])
        .replace('{dataCid}', deal.Ref.Root['/']);
      if (started) {
        do {
          await sleep(options.interval * 1000);
        } while (options.downloadConcurrency + options.importConcurrency -
        importSemaphore.getPermits() - downloadSemaphore.getPermits() > options.intervalCap);
      }
      started = true;
      (async () => {
        const success = await ImportUtil.download(url, pieceCidFile, options, downloadSemaphore);
        if (success) {
          await ImportUtil.importDeal(pieceCidFile, deal.ProposalCid, client, options, importSemaphore);
        }
      })();
    }
  }

}
