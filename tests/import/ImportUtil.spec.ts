import ImportOptions from '../../src/import/ImportOptions';
import { ErrorMessage } from '../../src/import/ErrorMessage';
import fs from 'fs-extra';
import ImportUtil from '../../src/import/ImportUtil';
import { sleep } from '../../src/common/Util';
import * as crypto from 'crypto';
import createSpyObj = jasmine.createSpyObj;
import MinerDeal from '../../src/import/MinerDeal';
import cloneDeep from 'lodash/cloneDeep';

describe('ImportUtil', () => {
  beforeEach(() => {
    process.env.MARKETS_API_INFO = 'eyJ:/ip4/1.1.1.1/tcp/1111/http';
  })
  const defaultOptions: ImportOptions = {
    since: 86400,
    urlTemplate: 'http://www.download.org/{dataCid}.car',
    downloadThreads: 4,
    downloadFolder: './downloads',
    removeImported: true,
    interval: 1,
    intervalCap: 4,
    downloadConcurrency: 2,
    importConcurrency: 2,
    dryRun: false,
    loop: false,
  };
  const defaultDeal: MinerDeal =
    {
      ProposalCid: {
        '/': 'proposal_cid'
      },
      CreationTime: new Date().toISOString(),
      State: 18,
      Ref: {
        Root: {
          '/': 'data_cid'
        },
        PieceCid: {
          '/': 'piece_cid'
        },
        PieceSize: 34359738368,
      },
      Proposal: {
        Client: 'f1client',
        Provider: 'f0miner'
      }
    };
  describe('validateImportOptions', () => {
    afterEach(() => {
      delete process.env.LOTUS_MINER_PATH;
      delete process.env.LOTUS_MARKETS_PATH;
      delete process.env.MINER_API_INFO;
      delete process.env.MARKETS_API_INFO;
    })
    it('should throw if all environment variables are not defined', async () => {
      delete process.env.LOTUS_MINER_PATH;
      delete process.env.LOTUS_MARKETS_PATH;
      delete process.env.MINER_API_INFO;
      delete process.env.MARKETS_API_INFO;
      const options = { ...defaultOptions };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.LOTUS_MINER_PATH_MISSING);
    })
    it('should throw if --since is <= 0', async () => {
      const options = {
        ...defaultOptions,
        since: 0
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.SINCE_LESS_THAN_0);
    });
    it('should throw if --path and --urlTemplate are not defined', async () => {
      const options = {
        ...defaultOptions,
        path: undefined,
        urlTemplate: undefined
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.PATH_OR_URL_TEMPLATE_REQUIRED);
    });
    it('should throw if --urlTemplate is defined and --downloadFolder is not defined', async () => {
      const options = {
        ...defaultOptions,
        urlTemplate: 'http://www.download.org/{dataCid>.car',
        downloadFolder: undefined
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.DOWNLOAD_FOLDER_REQUIRED);
    });
    it('should throw if --interval is < 0', async () => {
      const options = {
        ...defaultOptions,
        interval: -1
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.INTERVAL_LESS_THAN_0);
    });
    it('should throw if --intervalCap is < 1', async () => {
      const options = {
        ...defaultOptions,
        intervalCap: 0
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.INTERVAL_CAP_LESS_THAN_1);
    });
    it('should throw if --downloadConcurrency is < 1', async () => {
      const options = {
        ...defaultOptions,
        downloadConcurrency: 0
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.DOWNLOAD_CONCURRENCY_LESS_THAN_1);
    });
    it('should throw if --importConcurrency is < 1', async () => {
      const options = {
        ...defaultOptions,
        importConcurrency: 0
      };
      await expectAsync(ImportUtil['validateImportOptions'](options)).toBeRejectedWithError(ErrorMessage.IMPORT_CONCURRENCY_LESS_THAN_1);
    });
    it('should log an error if --importConcurrency is > 1', async () => {
      const spy = spyOn(console, 'warn');
      const options = {
        ...defaultOptions,
        importConcurrency: 3
      };
      await ImportUtil['validateImportOptions'](options);
      expect(spy).toHaveBeenCalledWith(ErrorMessage.IMPORT_CONCURRENCY_GREATER_THAN_1);
    });
    it('should log an error if --loop and --dryRun are used together', async () => {
      const spy = spyOn(console, 'warn');
      const options = {
        ...defaultOptions,
        loop: true,
        dryRun: true
      };
      await ImportUtil['validateImportOptions'](options);
      expect(spy).toHaveBeenCalledWith(ErrorMessage.LOOP_AND_DRY_RUN);
    });
    it('should return rpc client from api info', async () => {
      process.env.MARKETS_API_INFO = 'eyJ:/ip4/1.1.1.1/tcp/1111/http';
      const client = await ImportUtil['validateImportOptions'](defaultOptions);
      expect(client['url']).toEqual('http://1.1.1.1:1111/rpc/v0');
      expect(client['prefix']).toEqual('Filecoin.');
      expect(client['config']).toEqual({
        headers: {
          Authorization: 'Bearer eyJ'
        }
      });
    })
    it('should return rpc client from api info with localhost if the ip is 0.0.0.0', async () => {
      process.env.MARKETS_API_INFO = 'eyJ:/ip4/0.0.0.0/tcp/1111/http';
      const client = await ImportUtil['validateImportOptions'](defaultOptions);
      expect(client['url']).toEqual('http://127.0.0.1:1111/rpc/v0');
      expect(client['prefix']).toEqual('Filecoin.');
      expect(client['config']).toEqual({
        headers: {
          Authorization: 'Bearer eyJ'
        }
      });
    })
    it('should return rpc client from miner path', async () => {
      try {
        delete process.env.MARKETS_API_INFO;
        process.env.LOTUS_MINER_PATH = './testminer';
        await fs.mkdirp('./testminer');
        await fs.writeFile('./testminer/api', '/ip4/2.2.2.2/tcp/2222/http');
        await fs.writeFile('./testminer/token', 'eyJ');
        const client = await ImportUtil['validateImportOptions'](defaultOptions);
        expect(client['url']).toEqual('http://2.2.2.2:2222/rpc/v0');
        expect(client['prefix']).toEqual('Filecoin.');
        expect(client['config']).toEqual({
          headers: {
            Authorization: 'Bearer eyJ'
          }
        });
      } finally {
        await fs.rm('./testminer', { recursive: true, force: true });
      }
    })
  })
  describe('startImportLoop', () => {
    it('should call startImport once if --loop is falsy', async () => {
      const options = {
        ...defaultOptions,
        loop: false
      };
      const spy = spyOn<any>(ImportUtil, 'startImport');
      const aborted = false;
      await ImportUtil.startImportLoop(options, () => Promise.resolve(aborted));
      expect(spy).toHaveBeenCalledTimes(1);
    })
    it('should call startImport once if --dryRun is truthy', async () => {
      const options = {
        ...defaultOptions,
        loop: true,
        dryRun: true
      };
      const spy = spyOn<any>(ImportUtil, 'startImport');
      const aborted = false;
      await ImportUtil.startImportLoop(options, () => Promise.resolve(aborted));
      expect(spy).toHaveBeenCalledTimes(1);
    });
    it('should call startImport multiple times if --loop is truthy', async () => {
      const options = {
        ...defaultOptions,
        loop: true,
        interval: 0.1
      };
      const spy = spyOn<any>(ImportUtil, 'startImport');
      let aborted = false;
      ImportUtil.startImportLoop(options, () => Promise.resolve(aborted));
      await sleep(250);
      aborted = true;
      expect(spy).toHaveBeenCalledTimes(3);
      await sleep(200);
      expect(spy).toHaveBeenCalledTimes(3);
    })
  })
  describe('downloadFile', () => {
    it('should throw if the file cannot be downloaded', async () => {
      await expectAsync(ImportUtil['downloadFile']('http://127.0.0.1/none.txt', './none.txt', 4))
        .toBeRejectedWithError(/ECONNREFUSED/);
    })
    it('should be able to download a file with multithreading', async () => {
      try {
        await expectAsync(ImportUtil['downloadFile'](
          'https://github.com/tech-greedy/go-generate-car/releases/download/v2.1.2/go-generate-car_2.1.2_linux_amd64.tar.gz',
          './test_download.gz', 4)).toBeResolved();
        const content = await fs.readFile('./test_download.gz');
        const digest = crypto.createHash('sha256').update(content).digest('hex');
        expect(digest).toEqual('cc52af0fb9f3e5bbdcb74d519a584a06e21c5055b8da75ae5779dc08321caa2f');
      } finally {
        await fs.rm('./test_download.gz', { recursive: true, force: true });
      }
    });
  })
  describe('download', () => {
    it('should not actually download the file if --dryRun is truthy', async () => {
      const spy = spyOn<any>(ImportUtil, 'downloadFile');
      const options = {
        ...defaultOptions,
        dryRun: true
      };
      const spySem = createSpyObj('sem', ['acquire', 'release']);
      await ImportUtil['download']('http://something', './test_download.gz', options, spySem);
      expect(spy).not.toHaveBeenCalled();
      expect(spySem.acquire).toHaveBeenCalledTimes(1);
      expect(spySem.release).toHaveBeenCalledTimes(1);
    })
    it('should download the file', async () => {
      try {
        const spy = spyOn<any>(ImportUtil, 'downloadFile').and.callFake(async () => {
          await fs.createFile('./test_download.gz');
        });
        const options = {
          ...defaultOptions,
          dryRun: false
        };
        const spySem = createSpyObj('sem', ['acquire', 'release']);
        await ImportUtil['download']('http://something', './test_download.gz', options, spySem);
        expect(spy).toHaveBeenCalledTimes(1);
        expect(spySem.acquire).toHaveBeenCalledTimes(1);
        expect(spySem.release).toHaveBeenCalledTimes(1);
        expect(await fs.pathExists('./test_download.gz')).toBeTruthy();
      } finally {
        await fs.rm('./test_download.gz', { recursive: true, force: true });
      }
    })
  })
  describe('importDeal', () => {
    it('should not actually import the deal if --dryRun is truthy', async () => {
      const options = {
        ...defaultOptions,
        dryRun: true
      };
      const client = createSpyObj('client', ['call']);
      const spySem = createSpyObj('sem', ['acquire', 'release']);
      await ImportUtil['importDeal']('./test.car', { '/': 'bafg' }, client, options, spySem);
      expect(client.call).not.toHaveBeenCalled();
      expect(spySem.acquire).toHaveBeenCalledTimes(1);
      expect(spySem.release).toHaveBeenCalledTimes(1);
    })
    it('should import the deal', async () => {
      try {
        const options = {
          ...defaultOptions,
          dryRun: false,
          removeImported: true
        };
        const client = createSpyObj('client', { call: Promise.resolve({}) });
        const spySem = createSpyObj('sem', ['acquire', 'release']);
        await fs.createFile('./test.car');
        await ImportUtil['importDeal']('./test.car', { '/': 'bafg' }, client, options, spySem);
        expect(client.call).toHaveBeenCalled();
        expect(spySem.acquire).toHaveBeenCalledTimes(1);
        expect(spySem.release).toHaveBeenCalledTimes(1);
        expect(await fs.pathExists('./test.car')).toBeFalsy();
      } finally {
        await fs.rm('./test.car', { recursive: true, force: true });
      }
    });
  })

  describe('startImport', () => {
    it('should skip deals that does not match the client', async () => {
      const deal = cloneDeep(defaultDeal)
      const deals = { result: [deal] };
      const client = createSpyObj('client', {
        call: Promise.resolve(deals)
      });
      const options = {
        ...defaultOptions,
        client: ['some-other-client']
      }
      const downloadSpy = spyOn<any>(ImportUtil, 'download');
      const importSpy = spyOn<any>(ImportUtil, 'importDeal');
      await ImportUtil['startImport'](options, client);
      expect(downloadSpy).not.toHaveBeenCalled();
      expect(importSpy).not.toHaveBeenCalled();
    })
    it('should skip deals that does not match the State', async () => {
      const deal = cloneDeep(defaultDeal)
      deal.State = 19;
      const deals = { result: [deal] };
      const client = createSpyObj('client', {
        call: Promise.resolve(deals)
      });
      const options = {
        ...defaultOptions
      }
      const downloadSpy = spyOn<any>(ImportUtil, 'download');
      const importSpy = spyOn<any>(ImportUtil, 'importDeal');
      await ImportUtil['startImport'](options, client);
      expect(downloadSpy).not.toHaveBeenCalled();
      expect(importSpy).not.toHaveBeenCalled();
    })
    it('should skip deals that is too old', async () => {
      const deal = cloneDeep(defaultDeal)
      deal.CreationTime = new Date(Date.now() - (1000 * 60 * 60 * 30)).toISOString();
      const deals = { result: [deal] };
      const client = createSpyObj('client', {
        call: Promise.resolve(deals)
      });
      const options = {
        ...defaultOptions
      }
      const downloadSpy = spyOn<any>(ImportUtil, 'download');
      const importSpy = spyOn<any>(ImportUtil, 'importDeal');
      await ImportUtil['startImport'](options, client);
      expect(downloadSpy).not.toHaveBeenCalled();
      expect(importSpy).not.toHaveBeenCalled();
    })
    it('should skip download if the file already exists', async () => {
      try {
        const deal1 = cloneDeep(defaultDeal)
        const deal2 = cloneDeep(defaultDeal)
        const deals = { result: [deal1, deal2] };
        deal1.Ref.Root['/'] = 'data_cid1';
        deal2.Ref.PieceCid['/'] = 'piece_cid2';
        const client = createSpyObj('client', {
          call: Promise.resolve(deals)
        });
        const options = {
          ...defaultOptions,
          path: ['.']
        }
        await fs.createFile('./data_cid1.car')
        await fs.createFile('./piece_cid2.car')
        const downloadSpy = spyOn<any>(ImportUtil, 'download');
        const importSpy = spyOn<any>(ImportUtil, 'importDeal');
        await ImportUtil['startImport'](options, client);
        expect(downloadSpy).not.toHaveBeenCalled();
        expect(importSpy).toHaveBeenCalledWith('data_cid1.car', deal1.ProposalCid, client, options, jasmine.any(Object));
        expect(importSpy).toHaveBeenCalledWith('piece_cid2.car', deal2.ProposalCid, client, options, jasmine.any(Object));
      } finally {
        await fs.rm('./data_cid1.car', { recursive: true, force: true });
        await fs.rm('./piece_cid2.car', { recursive: true, force: true });
      }
    })
    it('should download files if the file does not exist', async () => {
      const deal1 = cloneDeep(defaultDeal)
      const deal2 = cloneDeep(defaultDeal)
      const deals = { result: [deal1, deal2] };
      deal2.Ref.Root['/'] = 'data_cid2';
      deal2.Ref.PieceCid['/'] = 'piece_cid2';
      const client = createSpyObj('client', {
        call: Promise.resolve(deals)
      });
      const options = {
        ...defaultOptions
      }
      const downloadSpy = spyOn<any>(ImportUtil, 'download').and.resolveTo(true);
      const importSpy = spyOn<any>(ImportUtil, 'importDeal');
      await ImportUtil['startImport'](options, client);
      expect(downloadSpy).toHaveBeenCalledWith('http://www.download.org/data_cid.car', 'downloads/data_cid.car', options, jasmine.any(Object));
      expect(downloadSpy).toHaveBeenCalledWith('http://www.download.org/data_cid2.car', 'downloads/data_cid2.car', options, jasmine.any(Object));
      expect(importSpy).toHaveBeenCalledWith('downloads/data_cid.car', deal1.ProposalCid, client, options, jasmine.any(Object));
      expect(importSpy).toHaveBeenCalledWith('downloads/data_cid2.car', deal2.ProposalCid, client, options, jasmine.any(Object));
    })

    it('should skip if urlTemplate is not defined', async () => {
        const deals = { result: [defaultDeal] };
        const client = createSpyObj('client', {
            call: Promise.resolve(deals)
        });
        const options = {
            ...defaultOptions,
            urlTemplate: undefined
        }
        const downloadSpy = spyOn<any>(ImportUtil, 'download');
        const importSpy = spyOn<any>(ImportUtil, 'importDeal');
        await ImportUtil['startImport'](options, client);
        expect(downloadSpy).not.toHaveBeenCalled();
        expect(importSpy).not.toHaveBeenCalled();
    })
  })
})
