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
    const fileLists = await Scanner.scan(request.path, request.minSize, request.maxSize);
    const fileListsToInsert = fileLists.map((fileList, index) => {
      const generationRequest = new Datastore.GenerationRequestModel();
      generationRequest.status = 'completed';
      generationRequest.datasetName = request.name;
      generationRequest.path = request.path;
      generationRequest.index = index;
      generationRequest.fileList = fileList;
      return generationRequest;
    });
    await Datastore.GenerationRequestModel.collection.insertMany(fileListsToInsert);
    this.logger.info('finished scanning');
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

  private async startPollWork (): Promise<void> {
    this.logger.info(`${this.workerId} - Polling for work`);
    const newScanningWork = await Datastore.ScanningRequestModel.findOneAndUpdate({
      workerId: null,
      completed: false
    }, {
      workerId: this.workerId
    });
    if (newScanningWork) {
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newScanningWork.name}`);
      await this.scan(newScanningWork);
      await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { completed: true });
    }

    const newGenerationWork = await Datastore.GenerationRequestModel.findOneAndUpdate({
      workerId: null,
      completed: false
    }, {
      workerId: this.workerId
    });
    if (newGenerationWork) {
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newGenerationWork.datasetName} [${newGenerationWork.index}]`);
      await this.generate(newGenerationWork);
      await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, { completed: true });
    }

    setTimeout(this.startPollWork, 5000);
  }

  private async startHealthCheck (): Promise<void> {
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

    setTimeout(this.startHealthCheck, 5000);
  }
}
