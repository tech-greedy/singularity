import { readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { spawn } from 'child_process';
import { randomUUID } from 'crypto';
import path from 'path';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest from '../common/model/GenerationRequest';
import ScanningRequest from '../common/model/ScanningRequest';
import Scanner from './Scanner';

interface UnixFsIpld {
  Name: string,
  Hash: string,
  Size: number,
  Link: UnixFsIpld[],
}

interface BodyShopOutput {
  cid: string,
  commp: string,
  ipld: UnixFsIpld
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
      this.logger.warn('Deal Preparation Worker is not enabled. Exit now...');
    }

    this.startHealthCheck();
    this.startPollWork();
  }

  private async scan (request: ScanningRequest): Promise<void> {
    let index = 0;
    for await (const fileList of Scanner.scan(request.path, request.minSize, request.maxSize)) {
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

  private async generate (request: GenerationRequest): Promise<void> {
    const input = JSON.stringify(request.fileList);
    const cmd = path.normalize(path.join(__dirname, '..', 'bodyshop'));
    const child = spawn(cmd, {
      stdio: ['pipe', 'pipe', process.stderr]
    });
    (async () => {
      await streamWrite(child.stdin, input);
      await streamEnd(child.stdin);
    })();
    const output :BodyShopOutput = JSON.parse(await readableToString(child.stdout));
    console.log(output);
    // const { cid, commp, ipld } = output;
    this.logger.info('finished generation');
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
      await this.scan(newScanningWork);
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
      await this.generate(newGenerationWork);
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { status: 'completed' });
    }

    return newGenerationWork != null;
  }

  private async startPollWork (): Promise<void> {
    const hasDoneWork = await this.pollWork();
    if (hasDoneWork) {
      setTimeout(this.startPollWork, 1);
    } else {
      setTimeout(this.startPollWork, 5000);
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
    this.logger.info(`${this.workerId} - Sending HealthCheck`);
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
