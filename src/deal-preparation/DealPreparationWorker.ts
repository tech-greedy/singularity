import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest from '../common/model/GenerationRequest';
import ScanningRequest from '../common/model/ScanningRequest';
import Scanner from './Scanner';
import config from 'config';
import fs from 'fs';
import path from 'path';

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[]
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
      this.logger.warn('Deal Preparation Worker is not enabled. Exit now...');
    }

    this.startHealthCheck();
    this.startPollWork();
  }

  private async scan (request: ScanningRequest): Promise<void> {
    let index = 0;
    for await (const fileList of Scanner.scan(request.path, request.minSize, request.maxSize)) {
      Scanner.buildSelector(request.path, fileList);
      await Datastore.GenerationRequestModel.create({
        datasetId: request.id,
        datasetName: request.name,
        path: request.path,
        index,
        fileList,
        status: 'active'
      });
      index++;
    }
    this.logger.info(`Finished scanning. Inserted ${index} tasks.`);
  }

  private async generate (request: GenerationRequest): Promise<[stdout: string, stderr: string, statusCode: number | null]> {
    const input = JSON.stringify(request.fileList.map(file => ({
      Path: file.path,
      Size: file.size,
      Start: file.start,
      End: file.end
    })));
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
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newScanningWork.name}`);
      try {
        await this.scan(newScanningWork);
      } catch (err) {
        if (err instanceof Error) {
          this.logger.error(`${this.workerId} - Encountered an error - ${err.message}`);
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
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newGenerationWork.datasetName} [${newGenerationWork.index}]`);
      const result = await this.generate(newGenerationWork);

      // Parse the output and update the database
      const [stdout, stderr, statusCode] = result!;
      if (statusCode !== 0) {
        this.logger.error(`${this.workerId} - Encountered an error - ${stderr}`);
        await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'error', errorMessage: stderr });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
        status: 'completed',
        dataCid: output.DataCid,
        pieceSize: output.PieceSize,
        pieceCid: output.PieceCid
      });
      this.logger.info(`${this.workerId} - Finished Generation of dataset: ${newGenerationWork.datasetName} [${newGenerationWork.index}]`);
    }

    return newGenerationWork != null;
  }

  private readonly PollInterval = 5000;

  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let hasDoneWork = false;
    try {
      hasDoneWork = await this.pollWork();
    } catch (err) {
      this.logger.crit(err);
    }
    if (hasDoneWork) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
  }

  private async pollWork (): Promise<boolean> {
    this.logger.info(`${this.workerId} - Polling for work`);
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
