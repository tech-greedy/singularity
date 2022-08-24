#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';
import fs from 'fs-extra';
import path from 'path';
import Scanner from './deal-preparation/scanner/Scanner';
import { FileInfo } from './common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import TaskQueue from '@goodware/task-queue';
import { randomUUID } from 'crypto';
import GenerateCar from './common/GenerateCar';
import { getContentsAndGroupings } from './deal-preparation/handler/GetGenerationManifestRequestHandler';
import { moveFileList, moveS3FileList } from './deal-preparation/worker/MoveProcessor';
import Logger, { Category } from './common/Logger';
import { generate, GenerateCarOutput, handleGeneratedFileList } from './deal-preparation/worker/GenerationProcessor';
import { validateCreatePreparationRequest } from './deal-preparation/handler/CreatePreparationRequestHandler';
import { ErrorMessage } from './deal-preparation/model/ErrorCode';

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
  .option('-f, --skip-inaccessible-files', 'Skip inaccessible files. Scanning may take longer to complete.')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').default('0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').default('0.95').argParser(parseFloat))
  .addOption(new Option('-j, --parallel <parallel>', 'How many generation jobs to run at the same time').default('1'))
  .action(async (name, p, outDir, options) => {
    GenerateCar.initialize();
    await fs.mkdir(outDir, { recursive: true });
    if (!p.startsWith('s3://')) {
      p = path.resolve(p);
    }
    outDir = path.resolve(outDir);

    const { minSize, maxSize, errorCode } = await validateCreatePreparationRequest(
      p, options.dealSize, outDir, options.minRatio,
      options.maxRatio, options.tmpDir);
    if (errorCode) {
      console.error(ErrorMessage[errorCode]);
      process.exit(1);
    }
    const queue = new TaskQueue({ workers: parseInt(options.parallel) });
    let task;
    const scanner = new Scanner();
    if (p.startsWith('s3://')) {
      await scanner.initializeS3Client(p);
    }
    for await (const fileList of scanner.scan(p, minSize!, maxSize!, undefined, Logger.getLogger(Category.Default), options.skipInaccessibleFiles)) {
      console.log('Pushed a new generation request');
      task = await queue.push(async () => {
        let tmpDir : string | undefined;
        if (options.tmpDir) {
          tmpDir = path.join(options.tmpDir, randomUUID());
        }
        let stdout;
        try {
          await fs.mkdir(outDir, { recursive: true });
          if (tmpDir) {
            if (p.startsWith('s3://')) {
              await moveS3FileList(Logger.getLogger(Category.Default), fileList, p, tmpDir);
            } else {
              await moveFileList(Logger.getLogger(Category.Default), fileList, p, tmpDir);
            }
            tmpDir = path.resolve(tmpDir);
          }
          const output = await generate(Logger.getLogger(Category.Default), {
            outDir, path: p, id: '', datasetName: '', datasetId: '', index: 0, status: 'active'
          }, fileList, tmpDir);
          stdout = output.stdout;
          if (output.code !== 0) {
            console.error(output.stderr);
            throw new Error(`Encountered error during generation. Program exited with code ${output.code}`);
          }
        } finally {
          if (tmpDir) {
            await fs.rm(tmpDir, { recursive: true });
          }
        }

        const output :GenerateCarOutput = JSON.parse(stdout?.toString() ?? '');
        const carFile = path.join(outDir, output.PieceCid + '.car');
        console.log(`Generated a new car ${carFile}`);
        const carFileStat = await fs.stat(carFile);
        const fileMap = new Map<string, FileInfo>();
        const parentPath = tmpDir ?? p;
        for (const fileInfo of fileList) {
          fileMap.set(path.relative(parentPath, fileInfo.path).split(path.sep).join('/'), fileInfo);
        }
        const generatedFileList = handleGeneratedFileList(fileMap, output.CidMap);

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
