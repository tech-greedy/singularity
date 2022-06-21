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

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[] | null
}

export interface GenerateCarOutput {
  Ipld: IpldNode,
  PieceSize: number,
  PieceCid: string,
  DataCid: string
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
    this.logger.debug(`Started scanning.`, { id: request.id, name: request.name, path: request.path, minSize: request.minSize, maxSize: request.maxSize });
    let index = 0;
    for await (const fileList of Scanner.scan(request.path, request.minSize, request.maxSize)) {
      const existing = await Datastore.GenerationRequestModel.findOne({ datasetId: request.id, index }, { _id: 1, status: 1 });
      if (existing) {
        if (existing.status === 'created') {
          await Datastore.GenerationRequestModel.findByIdAndDelete(existing.id, { projection: { _id: 1 } });
        } else {
          continue;
        }
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
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(generationRequest.id, {
        status: 'active'
      }, { projection: { _id: 1 } });
      index++;
      if ((await Datastore.ScanningRequestModel.findById(request.id))?.status === 'paused') {
        this.logger.info(`Scanning request has been paused.`);
        return;
      }
    }
    this.logger.debug(`Finished scanning. Inserted ${index} tasks.`);
    await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { status: 'completed' });
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
    const cmd = await fs.pathExists(path.join(__dirname, 'generate-car')) ? path.join(__dirname, 'generate-car') : 'generate-car';
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
          await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { status: 'error', errorMessage: err.message });
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
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'error', errorMessage: stderr }, { projection: { _id: 1 } });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      const carFile = path.join(newGenerationWork.outDir, output.DataCid + '.car');
      const carFileStat = await fs.stat(carFile);
      const fileMap = new Map<string, FileInfo>();
      for (const fileInfo of fileList) {
        fileMap.set(path.relative(newGenerationWork.path, fileInfo.path), fileInfo);
      }
      const generatedFileList: GeneratedFileList = [];
      await DealPreparationWorker.populateGeneratedFileList(fileMap, output.Ipld, [], [], generatedFileList);
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
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        status: 'completed',
        dataCid: output.DataCid,
        pieceSize: output.PieceSize,
        pieceCid: output.PieceCid,
        carSize: carFileStat.size
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

  public static async populateGeneratedFileList (
    fileMap: Map<string, FileInfo>,
    ipld: IpldNode, segments: string[],
    selector: number[],
    list: GeneratedFileList) : Promise<void> {
    const relPath = segments.length > 0 ? path.join(...segments) : '';
    if (ipld.Link == null) {
      const fileInfo = fileMap.get(relPath)!;
      list.push({
        path: relPath,
        dir: false,
        size: fileInfo.size,
        start: fileInfo.start,
        end: fileInfo.end,
        cid: ipld.Hash,
        selector: [...selector]
      });
    } else {
      list.push({
        path: relPath,
        dir: true,
        cid: ipld.Hash,
        selector: [...selector]
      });
      for (let linkId = 0; linkId < ipld.Link.length; ++linkId) {
        selector.push(linkId);
        segments.push(ipld.Link[linkId].Name);
        await DealPreparationWorker.populateGeneratedFileList(fileMap, ipld.Link[linkId], segments, selector, list);
        selector.pop();
        segments.pop();
      }
    }
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
