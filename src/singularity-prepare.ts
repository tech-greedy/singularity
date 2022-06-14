#!/usr/bin/env node

import packageJson from '../package.json';
import { Command, Option } from 'commander';
import fs from 'fs-extra';
import path from 'path';
import xbytes from 'xbytes';
import DealPreparationService from './deal-preparation/DealPreparationService';
import Scanner from './deal-preparation/Scanner';
import DealPreparationWorker, { GenerateCarOutput } from './deal-preparation/DealPreparationWorker';
import { FileInfo } from './common/model/InputFileList';
import { GeneratedFileList } from './common/model/OutputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import TaskQueue from '@goodware/task-queue';

const version = packageJson.version;
const program = new Command();
program.name('singularity-prepare')
  .version(version)
  .description('A tool to prepare dataset for slingshot')
  .argument('<datasetName>', 'Name of the dataset')
  .argument('<datasetPath>', 'Directory path to the dataset')
  .argument('<outDir>', 'The output Directory to save CAR files and manifest files')
  .requiredOption('-l, --url-prefix <urlPrefix>', 'The prefix of the download link, which will be followed by datacid.car, i.e. http://download.mysite.org/')
  .option('-s, --deal-size <deal_size>', 'Target deal size, i.e. 32GiB', '32 GiB')
  .addOption(new Option('-m, --min-ratio <min_ratio>', 'Min ratio of deal to sector size, i.e. 0.55').default('0.55').argParser(parseFloat))
  .addOption(new Option('-M, --max-ratio <max_ratio>', 'Max ratio of deal to sector size, i.e. 0.95').default('0.95').argParser(parseFloat))
  .addOption(new Option('-j, --parallel <parallel>', 'How many generation jobs to run at the same time').default('1'))
  .action(async (name, p, outDir, options) => {
    await fs.mkdir(outDir, { recursive: true });
    if (!await fs.pathExists(p)) {
      console.error(`Dataset path "${p}" does not exist.`);
      process.exit(1);
    }
    p = path.resolve(p);
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
    if (!options.urlPrefix.endsWith('/')) {
      options.urlPrefix = options.urlPrefix + '/';
    }

    const queue = new TaskQueue({ workers: parseInt(options.parallel) });
    let task;
    for await (const fileList of Scanner.scan(p, minSize, maxSize)) {
      console.log('Pushed a new generation request');
      task = await queue.push(async () => {
        const input = JSON.stringify(fileList.map(file => ({
          Path: file.path,
          Size: file.size,
          Start: file.start,
          End: file.end
        })));

        const [stdout, stderr, exitCode] = await DealPreparationWorker.invokeGenerateCar(input, outDir, p);
        if (exitCode !== 0) {
          console.error(stderr);
        }

        const output :GenerateCarOutput = JSON.parse(stdout);
        const carFile = path.join(outDir, output.DataCid + '.car');
        console.log(`Generated a new car ${carFile}`);
        const carFileStat = await fs.stat(carFile);
        const fileMap = new Map<string, FileInfo>();
        for (const fileInfo of fileList) {
          fileMap.set(path.relative(p, fileInfo.path), fileInfo);
        }
        const generatedFileList: GeneratedFileList = [];
        await DealPreparationWorker.populateGeneratedFileList(fileMap, output.Ipld, [], [], generatedFileList);

        const [contents, groupings] = DealPreparationService.getContentsAndGroupings(generatedFileList);

        const result = {
          piece_cid: output.PieceCid,
          payload_cid: output.DataCid,
          raw_car_file_size: carFileStat.size,
          car_file_link: options.urlPrefix + output.DataCid + '.car',
          dataset: name,
          contents,
          groupings
        };

        await fs.writeJSON(path.join(outDir, `${output.DataCid}.manifest`), result);
      });
    }
    if (task) {
      await task.promise;
    }
  });
program.parse();
