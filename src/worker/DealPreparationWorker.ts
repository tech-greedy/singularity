import { v4 as uuid } from 'uuid';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ScanningRequest from '../common/model/ScanningRequest';

export default class DealPreparationWorker extends BaseService {
  private readonly workerId: string;
  public constructor () {
    super(Category.DealPreparationWorker);
    this.workerId = uuid();
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

  private async startScanning (_request: ScanningRequest) : Promise<void> {
    this.logger.info('finished scanning');
  }

  private async startPollWork (): Promise<void> {
    this.logger.info(`${this.workerId} - Polling for work`);
    const newWork = await Datastore.ScanningRequestModel.findOneAndUpdate({
      workerId: null,
      completed: false
    }, {
      workerId: this.workerId
    }, {
      new: true
    });
    if (newWork) {
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newWork.datasetName}`);
      await this.startScanning(newWork);
      await Datastore.ScanningRequestModel.findByIdAndUpdate(newWork.id, { completed: true });
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
