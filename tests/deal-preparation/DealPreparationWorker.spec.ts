import path from 'path';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import DealPreparationWorker from '../../src/deal-preparation/DealPreparationWorker';
import Scanner from '../../src/deal-preparation/Scanner';
import fs from 'fs-extra';
import { FileList } from '../../src/common/model/InputFileList';
import { Stream } from 'stream';
import GenerateCar from '../../src/common/GenerateCar';
async function stream2buffer (stream: Stream): Promise<Buffer> {

  return new Promise < Buffer > ((resolve, reject) => {

    const _buf = Array < any > ();

    stream.on("data", chunk => _buf.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(_buf)));
    stream.on("error", err => reject(`error converting stream - ${err}`));

  });
}
describe('DealPreparationWorker', () => {
  let worker: DealPreparationWorker;
  let defaultTimeout: number;
  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealPreparationWorker();
    GenerateCar.initialize();
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
  });
  afterAll(async () => {
    for (const file of await fs.readdir('.')) {
      if (file.endsWith('.car')) {
        await fs.rm(file);
      }
    }
    await fs.rm('tests/subfolder1', { recursive: true, force: true });
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  })
  describe('moveFileList', () => {
    it('should move all local files to tmpdir', async () => {
      const fileList: FileList = [
        {
          path: './tests/test_folder/a/1.txt',
          size: 3
        },
        {
          path: './tests/test_folder/b/2.txt',
          size: 27,
          start: 12,
          end: 23
        }
      ]
      const tmpDir = './moveFileList-tests-tmp';
      await DealPreparationWorker.moveFileList(fileList, './tests/test_folder', tmpDir);
      expect((await fs.stat(tmpDir + '/a/1.txt')).size).toEqual(3);
      expect((await fs.stat(tmpDir + '/b/2.txt')).size).toEqual(11);
      expect((await fs.readFile(tmpDir + '/b/2.txt')).toLocaleString()).toEqual('hello world');
      expect(fileList[0].path).toEqual(path.resolve(tmpDir + '/a/1.txt'));
      expect(fileList[1].path).toEqual(path.resolve(tmpDir + '/b/2.txt'));
      await fs.rm(tmpDir, { recursive: true, force: true });
    })
  })
  describe('moveS3FileList', () => {
    it('should move all s3 files to tmpdir', async () => {
      const fileList: FileList = [
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx',
          size: 41464
        },
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
          start: 100,
          end: 200
        },
      ];
      const tmpDir = './moveFileList-tests-tmp';
      await DealPreparationWorker.moveS3FileList(fileList, 's3://gdc-beataml1.0-crenolanib-phs001628-2-open', tmpDir);
      const file1 = tmpDir + '/gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx';
      const file2 = tmpDir + '/gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx';
      expect((await fs.stat(file1)).size).toEqual(41464);
      expect((await fs.stat(file2)).size).toEqual(100);
      const buffer1 = await stream2buffer(fs.createReadStream(file1, { start: 100, end: 199 }));
      const buffer2 = await fs.readFile(file2);
      expect(buffer1).toEqual(buffer2);
      expect(fileList[0].path).toEqual(path.resolve(file1));
      expect(fileList[1].path).toEqual(path.resolve(file2));
      await fs.rm(tmpDir, { recursive: true, force: true });
    })
  })
  describe('startPollWork', () => {
    it('should immediately start next job if Scan work finishes', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(true);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(false);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).not.toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['ImmediatePollInterval']);
    })
    it('should immediately start next job if Generation work finishes', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(false);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(true);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['ImmediatePollInterval']);
    })
    it('should poll for next job after 5s if no work found', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(false);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(false);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['PollInterval']);
    })
  })
  describe('healthCheck', () => {
    it('should create an entry in HealthCheck table', async () => {
      await worker['healthCheck']();
      const found = await Datastore.HealthCheckModel.findOne({ workerId: worker['workerId'] });
      expect(found).not.toBeNull();
      expect(found!.workerId).toEqual(worker['workerId']);
      expect(found!.updatedAt).not.toBeNull();
    })
  })
  describe('pollWork', () => {
    it('should update with error if file no longer exists', async () => {
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        status: 'active',
        outDir: '.',
      });
      await Datastore.InputFileListModel.create({
        generationId: created.id,
        index: 0,
        fileList: [
          {
            path: 'tests/test_folder/not_exists.txt',
            size: 3,
            start: 0,
            end: 0
          }
        ]
      })
      await worker['pollWork']();
      const found = await Datastore.GenerationRequestModel.findById(created.id);
      expect<any>(found).toEqual(jasmine.objectContaining({
        status: 'error',
        errorMessage: jasmine.stringMatching(/no such file or directory|cannot find the file/)
      }));
    })

    // Unfortunately, this requires root to increase number of open files
    it('should generate commp, car files for dataset with > 10000 subfiles', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'completed'
      })
      await fs.mkdir('tests/subfolder1/subfolder2', { recursive: true });
      const fileList: FileList = []
      for (let i = 10000; i < 20000; ++i) {
        const p = path.join('tests/subfolder1/subfolder2', `${i}.txt`);
        await fs.writeFile(p, i.toString())
        fileList.push({
          path: p,
          size: (await fs.stat(p)).size
        })
      }
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: 'name',
        path: 'tests/subfolder1',
        index: 0,
        status: 'active',
        outDir: '.',
      });
      await Datastore.InputFileListModel.create({
        generationId: created.id,
        index: 0,
        fileList
      })
      await worker['pollWork']();
      const generation = await Datastore.GenerationRequestModel.findById(created.id);
      expect(generation).toEqual(jasmine.objectContaining({
        pieceCid: 'baga6ea4seaqfeodfhw5t6qpmwaisq2bdgkzzslokpauxr3qtfaxlite6l3y5ueq',
        dataCid: 'bafybeiguxstjbsehe6v6xppz3siy32sijzvi6s53qgqlzvbtvpelrqmyau',
        carSize: 1088493
      }));
      const list = await Datastore.OutputFileListModel.find({
        generationId: generation!.id
      });
      expect(list.length).toEqual(11);
      expect(list[0].generatedFileList[2].path).toEqual('subfolder2/10000.txt');
      expect(list[10].generatedFileList[0].path).toEqual('subfolder2/19998.txt');
    })
    it('should generate commp, car files', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'completed'
      })
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      });
      await Datastore.InputFileListModel.create({
        generationId: created.id,
        index: 0,
        fileList: [
          {
            path: 'tests/test_folder/a/1.txt',
            size: 3,
          },
          {
            path: 'tests/test_folder/b/2.txt',
            size: 27,
            start: 0,
            end: 9,
          }
        ]
      })
      await worker['pollWork']();
      const found = await Datastore.GenerationRequestModel.findById(created.id);
      expect(found).toEqual(jasmine.objectContaining({
        status: 'completed',
        dataCid: 'bafybeih2nwd66s7rstnbj4grzjw7re4lyhmx3auvphibbz7nalo4ygfypq',
        pieceCid: 'baga6ea4seaqoqixvkneyg6tzwfoqsmw33xdva3aywkawp6n5jd5tffjdmqrn6gy',
        pieceSize: 512,
      }));
      const generatedFileList = await Datastore.OutputFileListModel.findOne({
        generatedId: created.id
      });
      expect(generatedFileList).toEqual(jasmine.objectContaining({
        index: 0,
        generatedFileList: [
          jasmine.objectContaining({
            path: '',
            dir: true,
            cid: 'bafybeih2nwd66s7rstnbj4grzjw7re4lyhmx3auvphibbz7nalo4ygfypq',
          }),
          jasmine.objectContaining({
            path: 'a',
            dir: true,
            cid: 'bafybeifd34zco7545dzqflv7djpi3q2l2egi4l4coohgftgjssn4zoeu2y',
          }),
          jasmine.objectContaining({
            path: path.join('a', '1.txt'),
            size: 3,
            dir: false,
            cid: 'bafkreiey5jxe6ilpf62lnh77tm5ejbbmhbugzjuf6p2v3remlu73ced34q',
          }),
          jasmine.objectContaining({
            path: 'b',
            dir: true,
            cid: 'bafybeif7zaqg45xk5zvwybbfgeiotkzvjmd4bpjasb4aevne57dpt67com',
          }),
          jasmine.objectContaining({
            path: path.join('b', '2.txt'),
            size: 27,
            start: 0,
            end: 9,
            dir: false,
            cid: 'bafkreiblmv6wzk3grdk7u5a7u5zqh5vez3zatwuk3ptparw45unujqxysi',
          }),
        ]
      }))
    })
    it('should insert the database with fileLists', async () => {
      const created = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'tests/test_folder',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      expect(await worker['pollWork']()).toEqual(true);
      const found = await Datastore.ScanningRequestModel.findById(created.id);
      expect(found!.status).toEqual('completed');
      expect(await Datastore.GenerationRequestModel.find({ datasetId: created.id })).toHaveSize(4);
    })
    it('should update the database with error message if it counter any error', async () => {
      const created = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/home/shane/test_folder_not_exist',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      expect(await worker['pollWork']()).toEqual(true);
      expect(await Datastore.ScanningRequestModel.findById(created.id)).toEqual(jasmine.objectContaining({
        status: 'error',
        errorMessage: jasmine.stringContaining('ENOENT')
      }))
    })
  })
  describe('scan', () => {
    it('should start from last finished one', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: path.join('tests', 'test_folder'),
        minSize: 12,
        maxSize: 16,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir',
        scanned: 2
      });
      const active = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 0,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      })
      await Datastore.InputFileListModel.create({
        generationId: active.id,
        index: 0,
        fileList: [{
          path: path.join('tests', 'test_folder', 'a', '1.txt'),
          size: 3,
        }, {
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
        }],
      });
      await worker['scan'](scanning);
      expect((await Datastore.ScanningRequestModel.findById(scanning.id))!.scanned).toEqual(5);
      expect(await Datastore.GenerationRequestModel.findById(active.id)).not.toBeNull();
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 } });
      expect(requests.length).toEqual(3);
      expect(requests[requests.length - 1].index).toEqual(2);
      expect((await Datastore.InputFileListModel.findOne({generationId: requests[1].id}))!.fileList).toEqual([jasmine.objectContaining({
        path: path.join('tests', 'test_folder', 'c', '3.txt'),
        size: 9
      }),jasmine.objectContaining({
        path: path.join('tests', 'test_folder', 'd.txt'),
        size: 9,
        start: 0,
        end: 3
      })])
    })
    it('should delete pending generations and start from last one', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: path.join('tests', 'test_folder'),
        minSize: 12,
        maxSize: 16,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      });
      const active = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 0,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      })
      await Datastore.InputFileListModel.create({
        generationId: active.id,
        index: 0,
        fileList: [{
          path: path.join('tests', 'test_folder', 'a', '1.txt'),
          size: 3,
        }, {
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
          start: 0,
          end: 9,
        }],
      });
      const pending = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 1,
        status: 'created',
        outDir: '.',
        tmpDir: './tmpdir'
      })
      const pendingList = await Datastore.InputFileListModel.create({
        generationId: pending.id,
        index: 0,
        fileList: [{
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
          start: 9,
          end: 21,
        }],
      });
      await worker['scan'](scanning);
      expect(await Datastore.GenerationRequestModel.findById(pending.id)).toBeNull();
      expect(await Datastore.GenerationRequestModel.findById(active.id)).not.toBeNull();
      expect(await Datastore.InputFileListModel.findById(pendingList.id)).toBeNull();
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 } });
      expect(requests.length).toEqual(4);
      expect((await Datastore.InputFileListModel.findOne({generationId: requests[1].id}))!.fileList).toEqual([jasmine.objectContaining({
        path: path.join('tests', 'test_folder', 'b', '2.txt'),
        size: 27,
        start: 9,
        end: 21,
      })])
    })
    it('should work with >1000 fileList', async () => {
      let fileList: FileList = Array(5000).fill(null).map((_, i) => ({
        path: `tests/test_folder/folder/${i}.txt`,
        size: 100,
        start: 0,
        end: 0,
        dir: false
      }));
      const f = async function * () : AsyncGenerator<FileList> {
        yield fileList;
      };
      spyOn(Scanner, 'scan').and.returnValue(f());
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'tests/test_folder',
        minSize: 12,
        maxSize: 16,
        status: 'active',
        outDir: '.'
      })
      await worker['scan'](scanning);
      expect((await Datastore.ScanningRequestModel.findById(scanning.id))!.scanned).toEqual(5000);
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 } });
      expect(requests.length).toEqual(1);
      const list = await Datastore.InputFileListModel.find({generationId: requests[0].id}, null, {sort: {index: 1}});
      expect(list.length).toEqual(5);
      expect(list[0].fileList.length).toEqual(1000);
      expect(list[0].fileList[0].path).toEqual('tests/test_folder/folder/0.txt');
      expect(list[4].index).toEqual(4);
      expect(list[4].fileList[999].path).toEqual('tests/test_folder/folder/4999.txt');
    })
    it('should get the correct fileList', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: path.join('tests', 'test_folder'),
        minSize: 12,
        maxSize: 16,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      })
      await worker['scan'](scanning);
      expect((await Datastore.ScanningRequestModel.findById(scanning.id))!.scanned).toEqual(6);
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 } });
      /**
       * a/1.txt -> 3 bytes
       * b/2.txt -> 27 bytes
       * c/3.txt -> 9 bytes
       * d.txt   -> 9 bytes (symlink)
       * 0. a/1.txt (3) + b/2.txt (9) = 12
       * 1. b/2.txt(12) = 12
       * 2. b/2.txt(6) + c/3.txt(9) = 15
       * 3. d.txt(9) = 9
       */
      expect(requests.length).toEqual(4);
      expect(requests[0]).toEqual(jasmine.objectContaining({
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 0,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      }));
      expect(await Datastore.InputFileListModel.findOne({generationId: requests[0].id})).toEqual(jasmine.objectContaining({
        fileList: [jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'a', '1.txt'),
          size: 3,
        }), jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
          start: 0,
          end: 9,
        })],
      }))
      expect(requests[1]).toEqual(jasmine.objectContaining({
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 1,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      }));
      expect(await Datastore.InputFileListModel.findOne({generationId: requests[1].id})).toEqual(jasmine.objectContaining({
        fileList: [jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
          start: 9,
          end: 21,
        })],
      }));
      expect(requests[2]).toEqual(jasmine.objectContaining({
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 2,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      }));
      expect(await Datastore.InputFileListModel.findOne({generationId: requests[2].id})).toEqual(jasmine.objectContaining({
        fileList: [jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'b', '2.txt'),
          size: 27,
          start: 21,
          end: 27,
        }), jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'c', '3.txt'),
          size: 9,
        })],
      }));
      // windows does not support symbolic link
      const dtxtsize = process.platform === 'win32' ? 7 : 9;
      expect(requests[3]).toEqual(jasmine.objectContaining({
        datasetName: 'name',
        path: path.join('tests', 'test_folder'),
        index: 3,
        status: 'active',
        outDir: '.',
        tmpDir: './tmpdir'
      }));
      expect(await Datastore.InputFileListModel.findOne({generationId: requests[3].id})).toEqual(jasmine.objectContaining({
        fileList: [jasmine.objectContaining({
          path: path.join('tests', 'test_folder', 'd.txt'),
          size: dtxtsize,
        })]
      }));
    })
  })
})
