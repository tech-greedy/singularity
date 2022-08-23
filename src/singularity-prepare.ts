#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';
import fs from 'fs-extra';
import path from 'path';
import xbytes from 'xbytes';
import DealPreparationService from './deal-preparation/DealPreparationService';
import Scanner from './deal-preparation/scanner/Scanner';
import DealPreparationWorker, { GenerateCarOutput } from './deal-preparation/DealPreparationWorker';
import { FileInfo } from './common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import TaskQueue from '@goodware/task-queue';
import { randomUUID } from 'crypto';
import GenerateCar from './common/GenerateCar';
import { getContentsAndGroupings } from './deal-preparation/handler/GetGenerationManifestRequestHandler';

const version = packageJson.version;
const program = new Command();
program.name('singularity-prepare')
  .version(version)
  .description('A tool to prepare dataset for slingshot')
  .argument('<datasetName>', 'Name of the dataset')
  .argument('<datasetPath>', 'Directory path to the dataset')
  .argument('<outDir>', 'The output Directory to save CAR files and manifest files')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 32GiB', '32GiB')
  .option('-t, --tmp-dir <tmp_dir>', 'Optional temporary directory. May be useful when it is at least 2x faster than the dataset source, such as when the dataset is on network mount, and the I/O is the bottleneck')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').default('0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').default('0.95').argParser(parseFloat))
  .addOption(new Option('-j, --parallel <parallel>', 'How many generation jobs to run at the same time').default('1'))
  .action(async (name, p, outDir, options) => {
    GenerateCar.initialize();
    await fs.mkdir(outDir, { recursive: true });
    if (!p.startsWith('s3://') && !await fs.pathExists(p)) {
      console.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    if (p.startsWith('s3://') && !options.tmpDir) {
      console.error('tmp_dir needs to specified for S3 dataset');
      process.exit(1);
    }
    if (!p.startsWith('s3://')) {
      p = path.resolve(p);
    }
    outDir = path.resolve(outDir);
    const dealSize: string = options.dealSize;
    const minRatio: number = options.minRatio;
    const maxRatio: number = options.maxRatio;

    const dealSizeNumber = xbytes.parseSize(dealSize);
    // Validate dealSize
    if (!DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
      console.error(`Deal Size ${dealSize} is not valid`);
      process.exit(1);
    }
    if (minRatio && (minRatio < 0.5 || minRatio > 0.95)) {
      console.error(`minRatio Size ${minRatio} is not valid`);
      process.exit(1);
    }
    if (maxRatio && (maxRatio < 0.5 || maxRatio > 0.95)) {
      console.error(`maxRatio Size ${maxRatio} is not valid`);
      process.exit(1);
    }
    if (minRatio && maxRatio && minRatio >= maxRatio) {
      console.error(`maxRatio Size ${maxRatio} is not valid`);
      process.exit(1);
    }
    const minSize = Math.round(minRatio * dealSizeNumber);
    const maxSize = Math.round(maxRatio * dealSizeNumber);
    console.error(`Generating with minSize ${minSize}, maxSize ${maxSize}`);
    const queue = new TaskQueue({ workers: parseInt(options.parallel) });
    let task;
    for await (const fileList of Scanner.scan(p, minSize, maxSize)) {
      console.log('Pushed a new generation request');
      task = await queue.push(async () => {

        let tmpDir : string | undefined;
        if (options.tmpDir) {
          tmpDir = path.join(options.tmpDir, randomUUID());
        }
        let stdout, stderr, exitCode;
        try {
          await fs.mkdir(outDir, { recursive: true });
          if (tmpDir) {
            if (p.startsWith('s3://')) {
              await DealPreparationWorker.moveS3FileList(fileList, p, tmpDir);
            } else {
              await DealPreparationWorker.moveFileList(fileList, p, tmpDir);
            }
            tmpDir = path.resolve(tmpDir);
          }
          const input = JSON.stringify(fileList.map(file => ({
            Path: file.path,
            Size: file.size,
            Start: file.start,
            End: file.end
          })));
          const output = await DealPreparationWorker.invokeGenerateCar(undefined, input, outDir, tmpDir ?? p);
          stdout = output.stdout?.toString() ?? '';
          stderr = output.stderr?.toString() ?? '';
          exitCode = output.code;
        } finally {
          if (tmpDir) {
            await fs.rm(tmpDir, { recursive: true });
          }
        }

        if (exitCode !== 0) {
          console.error(stderr);
        }

        const output :GenerateCarOutput = JSON.parse(stdout);
        const carFile = path.join(outDir, output.PieceCid + '.car');
        console.log(`Generated a new car ${carFile}`);
        const carFileStat = await fs.stat(carFile);
        const fileMap = new Map<string, FileInfo>();
        const parentPath = tmpDir ?? p;
        for (const fileInfo of fileList) {
          fileMap.set(path.relative(parentPath, fileInfo.path).split(path.sep).join('/'), fileInfo);
        }
        const generatedFileList = DealPreparationWorker.handleGeneratedFileList(fileMap, output.CidMap);

        const [contents, groupings] = getContentsAndGroupings(generatedFileList);

        const result = {
          piece_cid: output.PieceCid,
          payload_cid: output.DataCid,
          raw_car_file_size: carFileStat.size,
          dataset: name,
          contents,
          groupings
        };

        await fs.writeJSON(path.join(outDir, `${output.PieceCid}.json`), result);
      });
    }
    if (task) {
      await task.promise;
    }
  });
program.parse();
