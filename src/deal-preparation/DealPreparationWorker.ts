import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest from '../common/model/GenerationRequest';
import ScanningRequest from '../common/model/ScanningRequest';
import Scanner from './Scanner';
import fs from 'fs-extra';
import path from 'path';
import { performance } from 'perf_hooks';
import { GeneratedFileList } from '../common/model/OutputFileList';
import { FileInfo } from '../common/model/InputFileList';
import GenerateCar from '../common/GenerateCar';

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[] | null
}

interface CidMapType {[key: string] : {
  IsDir: boolean, Cid: string
}}

export interface GenerateCarOutput {
  Ipld: IpldNode,
  PieceSize: number,
  PieceCid: string,
  DataCid: string,
  CidMap: CidMapType
}

export default class DealPreparationWorker extends BaseService {
  private readonly workerId: string;

  public constructor () {
    super(Category.DealPreparationWorker);
    this.workerId = randomUUID();
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
  }

  public start (): void {
    if (!this.enabled) {
      this.logger.warn('Worker is not enabled. Exit now...');
    }

    this.startHealthCheck();
    this.startPollWork();
  }

  private async scan (request: ScanningRequest): Promise<void> {
    this.logger.info(`Started scanning.`, { id: request.id, name: request.name, path: request.path, minSize: request.minSize, maxSize: request.maxSize });
    let index = 0;
    const deleted = (await Datastore.GenerationRequestModel.deleteMany({ datasetId: request.id, status: 'created' })).deletedCount;
    if (deleted > 0) {
      this.logger.info(`Deleted ${deleted} pending generation requests`);
    }
    const lastGeneration = await Datastore.GenerationRequestModel.findOne({ datasetId: request.id, status: { $ne: 'created' } }, { _id: 1, index: 1 }, { sort: { index: -1 } });
    let lastFile: string | undefined;
    if (lastGeneration) {
      const lastFileList = await Datastore.InputFileListModel.findOne({ generationId: lastGeneration.id, index: lastGeneration.index });
      if (!lastFileList) {
        this.logger.error('Found last generation but not the file list. Please report this as a bug.', { id: lastGeneration.id, index: lastGeneration.index });
        throw new Error('Found last generation but not the file list. Please report this as a bug.');
      }
      lastFile = lastFileList.fileList[lastFileList.fileList.length - 1].path;
      this.logger.info(`Resuming scanning. Skip creating generation request until ${lastFile} is reached.`);
    }
    for await (const fileList of Scanner.scan(request.path, request.minSize, request.maxSize)) {
      if (!await Datastore.ScanningRequestModel.findById(request.id)) {
        this.logger.info('The scanning request has been removed. Scanning stopped.', { id: request.id, name: request.name });
        return;
      }
      if ((await Datastore.ScanningRequestModel.findById(request.id))?.status === 'paused') {
        this.logger.info(`Scanning request has been paused.`, { id: request.id, name: request.name });
        return;
      }
      if (lastFile) {
        if (fileList.some(fileInfo => fileInfo.path === lastFile)) {
          this.logger.info(`Reached the last file ${lastFile}, resume creating generation request.`);
          lastFile = undefined;
        }
        index++;
        continue;
      }
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: request.id,
        datasetName: request.name,
        path: request.path,
        outDir: request.outDir,
        tmpDir: request.tmpDir,
        index,
        status: 'created'
      });
      this.logger.info('Created a new generation request.', { id: request.id, name: request.name, index });
      for (let i = 0; i < fileList.length; i += 1000) {
        await Datastore.InputFileListModel.updateOne({
          generationId: generationRequest.id,
          index: i / 1000
        },
        {
          $setOnInsert: {
            fileList: fileList.slice(i, i + 1000)
          }
        },
        {
          upsert: true,
          projection: { _id: 1 }
        });
        this.logger.debug('Created new INPUT file list for the generation request.', { id: request.id, name: request.name, index, from: i, to: i + 1000 });
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(generationRequest.id, {
        status: 'active'
      }, { projection: { _id: 1 } });
      this.logger.info('Marking generation request to active', { id: request.id, name: request.name, index });
      index++;
    }
    await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { status: 'completed', workerId: null });
    this.logger.info(`Finished scanning. Marking scanning to completed. Inserted ${index} generation tasks.`);
  }

  private async generate (request: GenerationRequest, input: string): Promise<[stdout: string, stderr: string, statusCode: number | null]> {
    await fs.mkdir(request.outDir, { recursive: true });
    this.logger.debug(`Spawning generate-car.`, { outPath: request.outDir, parentPath: request.path, tmpDir: request.tmpDir });
    let tmpDir: string | undefined;
    if (request.tmpDir) {
      tmpDir = path.join(request.tmpDir, randomUUID());
    }
    const [stdout, stderr, exitCode] = await DealPreparationWorker.invokeGenerateCar(input, request.outDir, request.path, tmpDir);
    if (tmpDir) {
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
    this.logger.debug(`Child process finished.`, { stdout, stderr, exitCode });
    return [stdout, stderr, exitCode];
  }

  public static async invokeGenerateCar (input: string, outDir: string, p: string, tmpDir?: string)
    : Promise<[stdout: string, stderr: string, statusCode: number | null]> {
    const cmd = GenerateCar.path!;
    const args = ['-o', outDir, '-p', p];
    if (tmpDir) {
      args.push('-t', tmpDir);
    }
    const child = spawn(cmd, args, {
      stdio: ['pipe', 'pipe', 'pipe']
    });
    (async () => {
      await streamWrite(child.stdin, input);
      await streamEnd(child.stdin);
    })();
    const stdout = await readableToString(child.stdout);
    let stderr = '';
    child.stderr.on('data', function (chunk) {
      stderr += chunk;
    });
    try {
      await onExit(child);
    } catch (_) {}
    return [stdout, stderr, child.exitCode];
  }

  private async pollScanningWork (): Promise<boolean> {
    const newScanningWork = await Datastore.ScanningRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId
    });
    if (newScanningWork) {
      this.logger.info(`${this.workerId} - Polled a new scanning request.`, { name: newScanningWork.name, id: newScanningWork.id });
      try {
        await this.scan(newScanningWork);
      } catch (err) {
        if (err instanceof Error) {
          this.logger.error(`${this.workerId} - Encountered an error.`, { error: err.message });
          await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { status: 'error', errorMessage: err.message, workerId: null });
          return true;
        }
        throw err;
      }
    }

    return newScanningWork != null;
  }

  private async pollGenerationWork (): Promise<boolean> {
    const newGenerationWork = await Datastore.GenerationRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId,
      generatedFileList: []
    }, {
      new: true
    });
    if (newGenerationWork) {
      this.logger.info(`${this.workerId} - Polled a new generation request.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
      const fileList = (await Datastore.InputFileListModel.find({
        generationId: newGenerationWork.id
      }, null, { sort: { index: 1 } })).map(r => r.fileList).flat();
      const input = JSON.stringify(fileList.map(file => ({
        Path: file.path,
        Size: file.size,
        Start: file.start,
        End: file.end
      })));
      let timeSpentInMs = performance.now();
      const result = await this.generate(newGenerationWork, input);
      timeSpentInMs = performance.now() - timeSpentInMs;

      // Parse the output and update the database
      const [stdout, stderr, statusCode] = result!;
      if (statusCode !== 0) {
        this.logger.error(`${this.workerId} - Encountered an error.`, { stderr });
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'error', errorMessage: stderr, workerId: null }, { projection: { _id: 1 } });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      const carFile = path.join(newGenerationWork.outDir, output.PieceCid + '.car');
      const carFileStat = await fs.stat(carFile);
      const fileMap = new Map<string, FileInfo>();
      for (const fileInfo of fileList) {
        fileMap.set(path.relative(newGenerationWork.path, fileInfo.path).split(path.sep).join('/'), fileInfo);
      }
      const generatedFileList = DealPreparationWorker.handleGeneratedFileList(fileMap, output.CidMap);
      if (!await Datastore.ScanningRequestModel.findById(newGenerationWork.datasetId)) {
        this.logger.info('Scanning request has been removed. Give up updating the generation request', { datasetId: newGenerationWork.datasetId, datasetName: newGenerationWork.datasetName });
        return true;
      }
      for (let i = 0; i < generatedFileList.length; i += 1000) {
        await Datastore.OutputFileListModel.updateOne({
          generationId: newGenerationWork.id,
          index: i / 1000
        },
        {
          $setOnInsert: {
            generatedFileList: generatedFileList.slice(i, i + 1000)
          }
        },
        {
          upsert: true,
          projection: { _id: 1 }
        });
        this.logger.debug('Created new OUTPUT file list for the generation request.', { id: newGenerationWork.id, name: newGenerationWork.datasetName, index: newGenerationWork.index, from: i, to: i + 1000 });
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        status: 'completed',
        dataCid: output.DataCid,
        pieceSize: output.PieceSize,
        pieceCid: output.PieceCid,
        carSize: carFileStat.size,
        errorMessage: null,
        workerId: null
      }, {
        projection: { _id: 1 }
      });
      await Datastore.InputFileListModel.deleteMany({
        generationId: newGenerationWork.id
      });
      this.logger.info(`${this.workerId} - Finished Generation of dataset.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index, timeSpentInMs: timeSpentInMs });
    }

    return newGenerationWork != null;
  }

  public static handleGeneratedFileList (
    fileMap: Map<string, FileInfo>,
    cidMap: CidMapType) : GeneratedFileList {
    const list: GeneratedFileList = [];
    for (const path in cidMap) {
      if (cidMap[path].IsDir) {
        list.push({
          path,
          dir: cidMap[path].IsDir,
          cid: cidMap[path].Cid
        });
        continue;
      }
      const fileInfo = fileMap.get(path)!;
      list.push({
        path,
        dir: cidMap[path].IsDir,
        cid: cidMap[path].Cid,
        size: fileInfo.size,
        start: fileInfo.start,
        end: fileInfo.end
      });
    }
    return list;
  }

  private readonly PollInterval = 5000;

  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let hasDoneWork = false;
    try {
      hasDoneWork = await this.pollWork();
    } catch (error) {
      this.logger.error(this.workerId, { error });
    }
    if (hasDoneWork) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
  }

  private async pollWork (): Promise<boolean> {
    this.logger.debug(`${this.workerId} - Polling for work`);
    let hasDoneWork = await this.pollScanningWork();
    if (!hasDoneWork) {
      hasDoneWork = await this.pollGenerationWork();
    }

    return hasDoneWork;
  }

  private async startHealthCheck (): Promise<void> {
    await this.healthCheck();
    setTimeout(this.startHealthCheck, 5000);
  }

  private async healthCheck (): Promise<void> {
    this.logger.debug(`${this.workerId} - Sending HealthCheck`);
    await Datastore.HealthCheckModel.findOneAndUpdate(
      {
        workerId: this.workerId
      },
      {},
      {
        upsert: true
      }
    );
  }
}
