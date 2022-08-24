import Datastore from '../../../src/common/Datastore';
import path from 'path';
import { FileList } from '../../../src/common/model/InputFileList';
import Scanner from '../../../src/deal-preparation/scanner/Scanner';
import Logger, { Category } from '../../../src/common/Logger';
import scan from '../../../src/deal-preparation/worker/ScanProcessor';
import Utils from '../../Utils';
import GenerateCar from '../../../src/common/GenerateCar';

async function createLargeFileListRequest () {
  let fileList: FileList = Array(5000).fill(null).map((_, i) => ({
    path: `tests/test_folder/folder/${i}.txt`,
    size: 100,
    start: 0,
    end: 0,
    dir: false
  }));
  const f = async function * (): AsyncGenerator<FileList> {
    yield fileList;
  };
  const scanner = new Scanner();
  spyOn(scanner, 'scan').and.returnValue(f());
  const scanning = await Datastore.ScanningRequestModel.create({
    name: 'name',
    path: 'tests/test_folder',
    minSize: 12,
    maxSize: 16,
    status: 'active',
    outDir: '.'
  })
  return { scanning, scanner };
}

fdescribe('ScanProcessor', () => {
  beforeAll(async () => {
    await Utils.initDatabase();
    GenerateCar.initialize();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
  });
  describe('scan', () => {
    it('should insert the database with fileLists', async () => {
      const created = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'tests/test_folder',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      await scan(Logger.getLogger(Category.Default), created, new Scanner());
      const found = await Datastore.ScanningRequestModel.findById(created.id);
      expect(found!.status).toEqual('completed');
      expect(await Datastore.GenerationRequestModel.find({ datasetId: created.id })).toHaveSize(4);
    })
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
      await scan(Logger.getLogger(Category.Default), scanning, new Scanner());
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
      await scan(Logger.getLogger(Category.Default), scanning, new Scanner());
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
    it('should stop scanning when the request is removed', async () => {
      const { scanning, scanner } = await createLargeFileListRequest();
      const scanPromise = scan(Logger.getLogger(Category.Default), scanning, scanner);
      await Datastore.ScanningRequestModel.findByIdAndDelete(scanning.id);
      await scanPromise;
      expect((await Datastore.GenerationRequestModel.find()).length).toEqual(0);
    })
    it('should stop scanning when the request is paused', async () => {
      const { scanning, scanner } = await createLargeFileListRequest();
      const scanPromise = scan(Logger.getLogger(Category.Default), scanning, scanner);
      await Datastore.ScanningRequestModel.findByIdAndUpdate(scanning.id, {status: 'paused'});
      await scanPromise;
      expect((await Datastore.GenerationRequestModel.find()).length).toEqual(1);
    })
    it('should work with >1000 fileList', async () => {
      const { scanning, scanner } = await createLargeFileListRequest();
      await scan(Logger.getLogger(Category.Default), scanning, scanner);
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
      await scan(Logger.getLogger(Category.Default), scanning, new Scanner());
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
