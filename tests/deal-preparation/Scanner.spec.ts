import Scanner from '../../src/deal-preparation/scanner/Scanner';
import path from 'path';
import Logger, { Category } from '../../src/common/Logger';

fdescribe('Scanner', () => {
  let defaultTimeout: number;
  const gatkScanner = new Scanner();
  const fastAiScanner = new Scanner();
  beforeAll(async () => {
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
    await gatkScanner.initializeS3Client('s3://gatk-sv-data-us-east-1');
    await fastAiScanner.initializeS3Client('s3://fast-ai-coco');
  })
  afterAll(() => {
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  })
  describe('detectS3Region', () => {
    it('should detect region us-east-1 for gatk-sv-data-us-east-1', async () => {
      await expectAsync(Scanner.detectS3Region('gatk-sv-data-us-east-1')).toBeResolvedTo('us-east-1');
    })
    it('should detect region us-east-2 for gatk-sv-data-us-east-2', async () => {
      await expectAsync(Scanner.detectS3Region('gatk-sv-data-us-east-2')).toBeResolvedTo('us-east-2');
    })
  })
  describe('scan', () => {
    it('should work with s3 path with lastFrom', async () => {
      let fileLists = [];
      for await (const fileList of fastAiScanner.scan('s3://fast-ai-coco', 20_000_000_000, 30_000_000_000, {
        path: 's3://fast-ai-coco/unlabeled2017.zip',
        size: 20126613414,
        start: 0,
        end: 1
      }, Logger.getLogger(Category.Default), false)) {
        fileLists.push(fileList);
      }
      expect(fileLists).toEqual([
        [
          {
            size: 20126613414,
            path: 's3://fast-ai-coco/unlabeled2017.zip',
            start: 1,
            end: 20126613414
          }
        ],
        [ { size: 815585330, path: 's3://fast-ai-coco/val2017.zip' } ]
      ]);
    })
    it('should work with s3 path', async () => {
      let fileLists = [];
      for await (const fileList of fastAiScanner.scan('s3://fast-ai-coco', 20_000_000_000, 30_000_000_000,
        undefined, Logger.getLogger(Category.Default), false)) {
        fileLists.push(fileList);
      }
      expect(fileLists.length).toEqual(3);
      expect(fileLists[0]).toEqual([
          {
            size: 252907541,
            path: 's3://fast-ai-coco/annotations_trainval2017.zip'
          },
          { size: 3245877008, path: 's3://fast-ai-coco/coco_sample.tgz' },
          { size: 801038, path: 's3://fast-ai-coco/coco_tiny.tgz' },
          { size: 2598183296, path: 's3://fast-ai-coco/giga-fren.tgz' },
          { size: 1144034, path: 's3://fast-ai-coco/image_info_test2017.zip' },
          {
            size: 4902013,
            path: 's3://fast-ai-coco/image_info_unlabeled2017.zip'
          },
          {
            size: 860725834,
            path: 's3://fast-ai-coco/panoptic_annotations_trainval2017.zip'
          },
          {
            size: 1148688564,
            path: 's3://fast-ai-coco/stuff_annotations_trainval2017.zip'
          },
          { size: 6646970404, path: 's3://fast-ai-coco/test2017.zip' },
          {
            size: 19336861798,
            start: 0,
            end: 5239800268,
            path: 's3://fast-ai-coco/train2017.zip'
          }
        ]
      );
      expect(fileLists[1]).toEqual([
          {
            size: 19336861798,
            start: 5239800268,
            end: 19336861798,
            path: 's3://fast-ai-coco/train2017.zip'
          },
          {
            size: 20126613414,
            start: 0,
            end: 5902938470,
            path: 's3://fast-ai-coco/unlabeled2017.zip'
          }
        ]
      );
      expect(fileLists[2]).toEqual([
          {
            size: 20126613414,
            start: 5902938470,
            end: 20126613414,
            path: 's3://fast-ai-coco/unlabeled2017.zip'
          },
          { size: 815585330, path: 's3://fast-ai-coco/val2017.zip' }
        ]
      );
    })
  })
  describe('listS3Path', () => {
    it('should work with public dataset', async () => {
      let entries = [];
      const scanner = new Scanner();
      await scanner.initializeS3Client('s3://gatk-sv-data-us-east-1');
      for await (const entry of scanner.listS3Path('s3://gatk-sv-data-us-east-1')) {
        entries.push(entry);
      }
      // Check there is no duplicate
      expect(new Set(entries.map(e => e.path)).size).toEqual(entries.length);
      // Check the result is sorted
      expect(entries.map(e => e.path).sort()).toEqual(entries.map(e => e.path));
      // Check the size is > 1000
      expect(entries.length).toEqual(6998);
      // Check the first and last one
      expect(entries[0]).toEqual({
        size: 41900756820,
        path: 's3://gatk-sv-data-us-east-1/bams/HG00096.final.bam'
      });
      expect(entries[entries.length - 1]).toEqual({
        size: 4079964,
        path: 's3://gatk-sv-data-us-east-1/reference/gvcf/NA21133.haplotypeCalls.er.raw.g.vcf.gz.tbi'
      });
      // Start from middle
      const startFrom = entries[3000];
      entries = [];
      for await (const entry of gatkScanner.listS3Path('s3://gatk-sv-data-us-east-1', startFrom)) {
        entries.push(entry);
      }
      expect(entries.length).toEqual(3998);
      expect(entries[0].path).toEqual(startFrom.path);
      expect(entries[entries.length - 1]).toEqual({
        size: 4079964,
        path: 's3://gatk-sv-data-us-east-1/reference/gvcf/NA21133.haplotypeCalls.er.raw.g.vcf.gz.tbi'
      });
    })
  })
  describe('scan', () => {
    it('should work without startFile', async () => {
      const arr = [];
      for await (const list of await new Scanner().scan(path.join('tests', 'test_folder'), 12, 16,
        undefined, Logger.getLogger(Category.Default), false)) {
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 3, path: 'tests/test_folder/a/1.txt' },
            { size: 27, start: 0, end: 9, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 9, end: 21, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 21, end: 27, path: 'tests/test_folder/b/2.txt' },
            { size: 9, path: 'tests/test_folder/c/3.txt' }
          ],
          [ { size: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
    it('should work with startFile of full file', async () => {
      const arr = [];
      for await (const list of await new Scanner().scan(path.join('tests', 'test_folder'), 12, 16, {
        size: 27, start: 0, end: 27, path: 'tests/test_folder/b/2.txt'
      }, Logger.getLogger(Category.Default), false)) {
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 9, path: 'tests/test_folder/c/3.txt' },
            { size: 9, start: 0, end: 3, path: 'tests/test_folder/d.txt' }
          ],
          [ { size: 9, start: 3, end: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
    it('should work with startFile of partial file', async () => {
      const arr = [];
      for await (const list of await new Scanner().scan(path.join('tests', 'test_folder'), 12, 16, {
        size: 27, start: 0, end: 9, path: 'tests/test_folder/b/2.txt'
      }, Logger.getLogger(Category.Default), false)) {
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 27, start: 9, end: 21, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 21, end: 27, path: 'tests/test_folder/b/2.txt' },
            { size: 9, path: 'tests/test_folder/c/3.txt' }
          ],
          [ { size: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
  })
})
