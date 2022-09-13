import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import fs from 'fs-extra';
import scan from './worker/ScanProcessor';
import TrafficMonitor from './worker/TrafficMonitor';
import { processGeneration } from './worker/GenerationProcessor';
import { AbortSignal } from '../common/AbortSignal';
import Scanner from './scanner/Scanner';

export default class DealPreparationWorker extends BaseService {
  public readonly trafficMonitor: TrafficMonitor;

  public constructor () {
    super(Category.DealPreparationWorker);
    this.trafficMonitor = new TrafficMonitor(1000);
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
  }

  public async start (): Promise<void> {
    if (!this.enabled) {
      this.logger.warn('Worker is not enabled. Exit now...');
    }
    await this.initialize();
    this.startHealthCheck();
    this.startPollWork();
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
      await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, { $set: { state: 'scanning' } });
      try {
        const scanner = new Scanner();
        if (newScanningWork.path.startsWith('s3://')) {
          await scanner.initializeS3Client(newScanningWork.path);
        }
        await scan(this.logger, newScanningWork, scanner);
      } catch (err) {
        if (err instanceof Error) {
          this.logger.error(`${this.workerId} - Encountered an error.`, err);
          await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { status: 'error', errorMessage: err.message, workerId: null });
          return true;
        }
        throw err;
      } finally {
        const modified = await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, { $set: { state: 'idle' } });
        this.logger.info(`[${this.workerId}] Set worker state to idle`, { modified });
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
      await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, { $set: { state: 'generation_start' } });
      this.logger.info(`${this.workerId} - Polled a new generation request.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
      let tmpDir : string | undefined;
      try {
        const output = await processGeneration(this, newGenerationWork);
        tmpDir = output.tmpDir;
        const finished = output.finished;
        if (!finished) {
          this.logger.info(`${this.workerId} - The generation did not finish.`,
            { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
        }
      } catch (error) {
        if (error instanceof Error) {
          await Datastore.GenerationRequestModel.findOneAndUpdate({ _id: newGenerationWork.id, status: 'active' }, { status: 'error', errorMessage: error.message, workerId: null });
        }
        this.logger.error(`${this.workerId} - Encountered an error.`, error);
      } finally {
        const modified = await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, { $set: { state: 'idle' } });
        this.logger.info(`[${this.workerId}] Set worker state to idle`, { modified });
        if (tmpDir) {
          await fs.rm(tmpDir, { recursive: true });
        }
      }
    }

    return newGenerationWork != null;
  }

  private readonly PollInterval = 5000;

  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let hasDoneWork = false;
    try {
      hasDoneWork = await this.pollWork();
    } catch (error) {
      this.logger.error(this.workerId, error);
    }
    if (hasDoneWork) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
  }

  private async pollWork (): Promise<boolean> {
    this.logger.debug(`${this.workerId} - Polling for work`);
    const health = await Datastore.HealthCheckModel.findOne({ workerId: this.workerId });
    if (health?.state !== 'idle') {
      this.logger.info(`${this.workerId} - Not ready yet.`, { health });
      return false;
    }
    let hasDoneWork = await this.pollScanningWork();
    if (!hasDoneWork) {
      hasDoneWork = await this.pollGenerationWork();
    }

    return hasDoneWork;
  }

  private async startHealthCheck (abortSignal?: AbortSignal): Promise<void> {
    if (abortSignal && await abortSignal()) {
      return;
    }
    await this.healthCheck();
    setTimeout(async () => this.startHealthCheck(abortSignal), 5000);
  }

  private async healthCheck (): Promise<void> {
    this.logger.debug(`${this.workerId} - Sending HealthCheck`);
    await Datastore.HealthCheckModel.findOneAndUpdate(
      {
        workerId: this.workerId
      },
      {
        $set: {
          downloadSpeed: this.trafficMonitor.downloaded
        }
      }
    );
  }
}
