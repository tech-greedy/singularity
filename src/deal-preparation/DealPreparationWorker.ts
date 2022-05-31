import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest, { FileList } from '../common/model/GenerationRequest';
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
      const segments = path.relative(request.path, fileList[0].path).split(path.sep);
      let newPath = request.path;
      const newList: FileList = [];
      for (let i = 0; i < segments.length; ++i) {
        newList.push({
          path: newPath,
          selector: [],
          dir: true,
          end: 0,
          size: 0,
          start: 0
        });
        newPath = path.join(newPath, segments[i]);
      }
      fileList.unshift(...newList);
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
        }, { projection: { fileList: 0 } });
      }
      index++;
    }
    this.logger.debug(`Finished scanning. Inserted ${index} tasks.`);
  }

  private async generate (request: GenerationRequest): Promise<[stdout: string, stderr: string, statusCode: number | null]> {
    const input = JSON.stringify(request.fileList.filter(file => !file.dir).map(file => ({
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
      await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { status: 'completed' });
    }

    return newScanningWork != null;
  }

  private async pollGenerationWork (): Promise<boolean> {
    const newGenerationWork = await Datastore.GenerationRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId
    });
    if (newGenerationWork) {
      this.logger.info(`${this.workerId} - Polled a new generation request.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
      const result = await this.generate(newGenerationWork);

      // Parse the output and update the database
      const [stdout, stderr, statusCode] = result!;
      if (statusCode !== 0) {
        this.logger.error(`${this.workerId} - Encountered an error.`, { stderr });
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'error', errorMessage: stderr }, { projection: { fileList: 0 } });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      const carFile = path.join(this.outPath, output.DataCid + '.car');
      const carFileStat = await fs.stat(carFile);
      await this.updateFileList(newGenerationWork.id, output.Ipld, 0, []);
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        status: 'completed',
        dataCid: output.DataCid,
        pieceSize: output.PieceSize,
        pieceCid: output.PieceCid,
        carSize: carFileStat.size
      }, { projection: { fileList: 0 } });
      this.logger.info(`${this.workerId} - Finished Generation of dataset.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
    }

    return newGenerationWork != null;
  }

  private async updateFileList (id: string, ipld: IpldNode, idx: number, selector: number[]) : Promise<number> {
    await Datastore.GenerationRequestModel.findByIdAndUpdate(id, {
      $set: {
        [`fileList.${idx}.cid`]: ipld.Hash,
        [`fileList.${idx}.selector`]: selector
      }
    }, { projection: { id: 1 } });
    idx += 1;
    if (ipld.Link != null) {
      for (let linkId = 0; linkId < ipld.Link.length; ++linkId) {
        selector.push(linkId);
        idx = await this.updateFileList(id, ipld.Link[linkId], idx, selector);
        selector.pop();
      }
    }
    return idx;
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
