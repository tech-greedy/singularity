import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest, { FileInfo, GeneratedFileList } from '../common/model/GenerationRequest';
import ScanningRequest from '../common/model/ScanningRequest';
import Scanner from './Scanner';
import config from 'config';
import fs from 'fs-extra';
import path from 'path';

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[] | null
}

interface GenerateCarOutput {
  Ipld: IpldNode,
  PieceSize: number,
  PieceCid: string,
  DataCid: string
}

export default class DealPreparationWorker extends BaseService {
  private readonly workerId: string;
  private readonly outPath: string;

  public constructor () {
    super(Category.DealPreparationWorker);
    this.workerId = randomUUID();
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
    this.outPath = path.resolve(process.env.NODE_CONFIG_DIR!, config.get('deal_preparation_worker.out_dir'));
    fs.mkdirSync(this.outPath, { recursive: true });
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
      const existing = await Datastore.GenerationRequestModel.findOne({ datasetId: request.id, index }, { _id: 1 });
      if (existing) {
        continue;
      }
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: request.id,
        datasetName: request.name,
        path: request.path,
        index,
        fileList: fileList.slice(0, 1000),
        status: 'active'
      });
      for (let i = 1000; i < fileList.length; i += 1000) {
        await Datastore.GenerationRequestModel.findByIdAndUpdate(generationRequest.id, {
          $push: {
            fileList: {
              $each: fileList.slice(i, i + 1000)
            }
          }
        }, { projection: { _id: 1 } });
      }
      index++;
      if ((await Datastore.ScanningRequestModel.findById(request.id))?.status === 'paused') {
        this.logger.info(`Scanning request has been paused.`);
        return;
      }
    }
    this.logger.debug(`Finished scanning. Inserted ${index} tasks.`);
    await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { status: 'completed' });
  }

  private async generate (request: GenerationRequest): Promise<[stdout: string, stderr: string, statusCode: number | null]> {
    const input = JSON.stringify(request.fileList.map(file => ({
      Path: file.path,
      Size: file.size,
      Start: file.start,
      End: file.end
    })));
    this.logger.debug(`Spawning generate-car.`, { outPath: this.outPath, parentPath: request.path });
    const child = spawn('generate-car', ['-o', this.outPath, '-p', request.path], {
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
    this.logger.debug(`Child process finished.`, { stdout, stderr, exitCode: child.exitCode });
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
      const result = await this.generate(newGenerationWork);

      // Parse the output and update the database
      const [stdout, stderr, statusCode] = result!;
      if (statusCode !== 0) {
        this.logger.error(`${this.workerId} - Encountered an error.`, { stderr });
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'error', errorMessage: stderr }, { projection: { _id: 1 } });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      const carFile = path.join(this.outPath, output.DataCid + '.car');
      const carFileStat = await fs.stat(carFile);
      const fileMap = new Map<string, FileInfo>();
      for (const fileInfo of newGenerationWork.fileList) {
        fileMap.set(path.relative(newGenerationWork.path, fileInfo.path), fileInfo);
      }
      const generatedFileList: GeneratedFileList = [];
      await this.populateGeneratedFileList(fileMap, output.Ipld, [], [], generatedFileList);
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        generatedFileList: generatedFileList.slice(0, 1000)
      }, {
        projection: { _id: 1 }
      });
      for (let i = 1000; i < generatedFileList.length; i += 1000) {
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
          $push: {
            generatedFileList: {
              $each: generatedFileList.slice(i, i + 1000)
            }
          }
        }, {
          projection: { _id: 1 }
        });
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        status: 'completed',
        dataCid: output.DataCid,
        pieceSize: output.PieceSize,
        pieceCid: output.PieceCid,
        carSize: carFileStat.size,
        fileList: []
      }, {
        projection: { _id: 1 }
      });
      this.logger.info(`${this.workerId} - Finished Generation of dataset.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
    }

    return newGenerationWork != null;
  }

  private async populateGeneratedFileList (
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
        size: 0,
        start: 0,
        end: 0,
        cid: ipld.Hash,
        selector: [...selector]
      });
      for (let linkId = 0; linkId < ipld.Link.length; ++linkId) {
        selector.push(linkId);
        segments.push(ipld.Link[linkId].Name);
        await this.populateGeneratedFileList(fileMap, ipld.Link[linkId], segments, selector, list);
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
      this.logger.crit(this.workerId, { error });
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
