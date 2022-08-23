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
