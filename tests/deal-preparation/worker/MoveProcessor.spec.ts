import { FileList } from '../../../src/common/model/InputFileList';
import fs from 'fs-extra';
import path from 'path';
import { moveFileList, moveS3FileList } from '../../../src/deal-preparation/worker/MoveProcessor';
import Logger, { Category } from '../../../src/common/Logger';
import { Stream } from 'stream';

async function stream2buffer (stream: Stream): Promise<Buffer> {
  return new Promise < Buffer > ((resolve, reject) => {
    const _buf = Array < any > ();
    stream.on("data", chunk => _buf.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(_buf)));
    stream.on("error", err => reject(`error converting stream - ${err}`));

  });
}

describe('MoveProcessor', () => {
  describe('moveFileList', () => {
    it('should skip inaccessible files', async () => {
      try {
        await fs.mkdirp('/tmp/unittest');
        await fs.writeFile('/tmp/unittest/test.txt', 'test');
        await fs.chmod('/tmp/unittest/test.txt', 0);
        const fileList: FileList = [
          {
            path: '/tmp/unittest/test.txt',
            size: 4
          }
        ];
        const parentDir = '/tmp/unittest';
        const tmpDir = '/tmp/tmpdir';
        const moveResult = await moveFileList(Logger.getLogger(Category.Default), fileList, parentDir, tmpDir, true);
        expect(moveResult.aborted).toEqual(false)
        expect(moveResult.skipped.size).toEqual(1);
        expect(await fs.readdir(tmpDir)).toEqual([]);
      } finally {
        await fs.rm('/tmp/unittest', { recursive: true });
        await fs.rm('/tmp/tmpdir', { recursive: true });
      }
    })
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
      await moveFileList(Logger.getLogger(Category.Default), fileList, './tests/test_folder', tmpDir);
      expect((await fs.stat(tmpDir + '/a/1.txt')).size).toEqual(3);
      expect((await fs.stat(tmpDir + '/b/2.txt')).size).toEqual(11);
      expect((await fs.readFile(tmpDir + '/b/2.txt')).toLocaleString()).toEqual('hello world');
      expect(fileList[0].path).toEqual(path.resolve(tmpDir + '/a/1.txt'));
      expect(fileList[1].path).toEqual(path.resolve(tmpDir + '/b/2.txt'));
      await fs.rm(tmpDir, { recursive: true, force: true });
    })
    it('should skip moving inaccessible files for local move', async () => {
      const fileList: FileList = [{
        path: './not-existing-file',
        size: 100
      }];
        const tmpDir = './moveFileList-tests-tmp3';
        const moveResult = await moveFileList(Logger.getLogger(Category.Default), fileList, '.', tmpDir, true);
        expect(moveResult.aborted).toEqual(false);
        expect(moveResult.skipped.size).toEqual(1);
        await fs.rm(tmpDir, { recursive: true, force: true });
    })
  })
  describe('moveS3FileList', () => {
    it('should skip moving inaccessible files', async () => {
      const fileList: FileList = [{
        path: 's3://lab41openaudiocorpus/test.txt',
        size: 0
      }];
      const tmpDir = './moveFileList-tests-tmp2';
      const moveResult = await moveS3FileList(Logger.getLogger(Category.Default), fileList, 's3://lab41openaudiocorpus', tmpDir, true);
      expect(moveResult.aborted).toEqual(false)
      expect(moveResult.skipped.size).toEqual(1);
      expect((await fs.readdir(path.join(tmpDir, 'lab41openaudiocorpus')))).toEqual([]);
      await fs.rm(tmpDir, { recursive: true, force: true });
    })
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
      await moveS3FileList(Logger.getLogger(Category.Default), fileList, 's3://gdc-beataml1.0-crenolanib-phs001628-2-open', tmpDir);
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
})
