/* eslint @typescript-eslint/no-var-requires: "off" */
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ReplicationRequest from '../common/model/ReplicationRequest';
import config from 'config';
import axios from 'axios';
import { create, all, Unit } from 'mathjs';
import GenerationRequest from '../common/model/GenerationRequest';
const mathconfig = {};
const math = create(all, mathconfig);
const exec: any = require('await-exec');// no TS support

export default class DealReplicationWorker extends BaseService {
  private readonly workerId: string;
  private readonly lotusCMD: string;
  private readonly boostCMD: string;

  public constructor () {
    super(Category.DealReplicationWorker);
    this.workerId = randomUUID();
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
    this.lotusCMD = config.get('deal_replication_worker.lotus_cli_cmd');
    this.boostCMD = config.get('deal_replication_worker.boost_cli_cmd');
  }

  public start (): void {
    if (!this.enabled) {
      this.logger.warn('Deal Replication Worker is not enabled. Exit now...');
    }

    this.startHealthCheck();
    this.startPollWork();
  }

  private readonly PollInterval = 5000;

  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let hasDoneWork = false;
    try {
      hasDoneWork = await this.pollWork();
    } catch (err) {
      this.logger.error('Pool work error', err);
    }
    if (hasDoneWork) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
  }

  private async pollWork (): Promise<boolean> {
    this.logger.debug(`${this.workerId} - Polling for work`);
    const hasDoneWork = await this.pollReplicationWork();
    return hasDoneWork;
  }

  private async pollReplicationWork (): Promise<boolean> {
    const newReplicationWork = await Datastore.ReplicationRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId
    });
    if (newReplicationWork) {
      this.logger.info(`${this.workerId} - Received a new request - dataset: ${newReplicationWork.datasetId}`);
      try {
        await this.replicate(newReplicationWork);
      } catch (err) {
        if (err instanceof Error) {
          this.logger.error(`${this.workerId} - Encountered an error - ${err.message}`);
          await Datastore.ReplicationRequestModel.findByIdAndUpdate(newReplicationWork.id, { status: 'error', errorMessage: err.message });
          return true;
        }
        throw err;
      }
      await Datastore.ReplicationRequestModel.findByIdAndUpdate(newReplicationWork.id, { status: 'completed' });
    }

    return newReplicationWork != null;
  }

  /**
   * Create providers list by criteria. Currently it is just a list of providers separated by comma.
   * TODO marking this function as async pending future Pando integration
   *
   * @param criteria
   * @returns
   */
  private static async generateProvidersList (criteria: string): Promise<Array<string>> {
    return criteria.split(',');
  }

  /**
   * Check if given provider is using lotus or boost
   *
   * @param provider
   * @returns true is lotus, false is boost
   */
  private async isUsingLotus (provider: string): Promise<boolean> {
    let useLotus = true;
    // use boost libp2p command to check whether provider supports boost
    const versionQueryCmd = `${this.boostCMD} provider libp2p-info ${provider}`;
    const cmdOut = await exec(versionQueryCmd);
    if (cmdOut.stdout.includes('/fil/storage/mk/1.2.0')) {
      useLotus = false;
      this.logger.debug(`SP ${provider} supports boost.`);
    } else if (cmdOut.stdout.includes('/fil/storage/mk/1.1.0')) {
      this.logger.debug(`SP ${provider} supports lotus.`);
    } else {
      throw new Error(JSON.stringify(cmdOut));
    }
    return useLotus;
  }

  private static calculatePriceWithSize (price: number, pieceSize: number): Unit {
    if (price > 0) {
      return math.unit(price * (pieceSize || 0) / 1073741824, 'm');
    } else {
      return math.unit(0, 'm');
    }
  }

  /**
   * TODO: assemble URL with builtin hosting service
   * @param model
   * @param carFile
   * @returns
   */
  private static assembleDownloadUrl (model: ReplicationRequest, carFile: GenerationRequest) {
    let downloadUrl = model.urlPrefix;
    if (!downloadUrl.endsWith('/')) {
      downloadUrl += '/';
    }
    let carFilename = carFile.dataCid + '.car';
    if (carFile.filenameOverride) {
      carFilename = carFile.filenameOverride;
      if (!carFile.filenameOverride.endsWith('.car')) {
        carFilename += '.car';
      }
    }
    downloadUrl += carFilename;
    return downloadUrl;
  }

  private async checkUrlReachability (url: string): Promise<boolean> {
    try {
      await axios.head(url);
      return true;
    } catch (error) {
      this.logger.error(`This download URL ${url} is not reachable.`);
      return false;
    }
  }

  private async createDealCmd (useLotus: boolean, provider: string, replicationRequest: ReplicationRequest, carFile: GenerationRequest): Promise<string> {
    if (useLotus) {
      if (replicationRequest.isOffline) {
        const priceInFilWithSize = DealReplicationWorker.calculatePriceWithSize(replicationRequest.maxPrice, carFile.pieceSize!);
        const unpaddedSize = carFile.pieceSize! * 127 / 128;
        return `${this.lotusCMD} client deal --manual-piece-cid=${carFile.pieceCid} --manual-piece-size=${unpaddedSize} ` +
          `--manual-stateless-deal --from=${replicationRequest.client} --verified-deal=${replicationRequest.isVerfied} ` +
          `${carFile.dataCid} ${provider} ${priceInFilWithSize.toNumber()} ${replicationRequest.duration}`;
      } else {
        throw new Error(`Deal making failed. SP ${provider} only supports lotus and for lotus we only support offline deals.`);
      }
    } else {
      // determine car download link
      const downloadUrl = DealReplicationWorker.assembleDownloadUrl(replicationRequest, carFile);
      // check if download URL is reachable if necessary
      if (!replicationRequest.isOffline && !(await this.checkUrlReachability(downloadUrl))) {
        throw new Error(`${downloadUrl} is not reachable`);
      }
      const priceInAttoWithoutSize = math.unit(replicationRequest.maxPrice, 'm').toNumber('am');
      let propose = `deal --http-url=${downloadUrl}`;
      if (replicationRequest.isOffline) {
        propose = `offline-deal`;
      }
      return `${this.boostCMD} ${propose} --provider=${provider}  --commp=${carFile.pieceCid} --car-size=${carFile.carSize} ` +
        `--piece-size=${carFile.pieceSize} --payload-cid=${carFile.dataCid} --storage-price=${priceInAttoWithoutSize} ` +
        `--verified=${replicationRequest.isVerfied} --wallet=${replicationRequest.client} --duration=${replicationRequest.duration}`;
    }
  }

  /**
   * Main function of deal making
   * @param replicationRequest
   */
  private async replicate (replicationRequest: ReplicationRequest): Promise<void> {
    const providers = await DealReplicationWorker.generateProvidersList(replicationRequest.criteria);
    const boostResultUUIDMatcher = /deal uuid: (\S+)/;
    let breakOuter = false;
    for (let i = 0; i < providers.length; i++) {
      if (breakOuter) {
        break;
      }
      const provider = providers[i];
      let useLotus = true;
      try {
        useLotus = await this.isUsingLotus(provider);
      } catch (error) {
        this.logger.error(`SP ${provider} unknown output from libp2p. Give up on this SP.`, error);
        continue;
      }

      // Find cars that are finished generation
      const cars = await Datastore.GenerationRequestModel.find({
        datasetId: replicationRequest.datasetId,
        status: 'completed'
      });

      let dealsMadePerSP = 0;
      let retryTimeout = config.get<number>('deal_replication_worker.min_retry_wait_ms'); // 30s, 60s, 120s ...
      for (let j = 0; j < cars.length; j++) {
        const carFile = cars[j];

        // check if the replication request has been paused
        const existingRec = await Datastore.ReplicationRequestModel.findById(replicationRequest.id);
        if (!existingRec) {
          this.logger.error(`This replication request ${replicationRequest.id} no longer exist.`);
          breakOuter = true;
          break;
        }
        if (existingRec.status !== 'active') {
          this.logger.warn(`This replication request ${existingRec.id} has become non-active: ${existingRec.status}.`);
          breakOuter = true;
          break;
        }

        // check if reached max deals needed to be sent
        if (existingRec.maxNumberOfDeals > 0 && dealsMadePerSP >= existingRec.maxNumberOfDeals) {
          this.logger.warn(`This SP ${provider} has made max deals planned (${existingRec.maxNumberOfDeals}) by the request ${existingRec.id}.`);
          break;
        }

        // check if the car has already dealt or have enough replica
        const existingDeals = await Datastore.DealStateModel.find({
          pieceCid: carFile.pieceCid,
          state: { $nin: ['slashed', 'error'] }
        });
        let alreadyDealt = false;
        for (let k = 0; k < existingDeals.length; k++) {
          const deal = existingDeals[k];
          if (deal.provider === provider) {
            this.logger.warn(`This pieceCID ${carFile.pieceCid} has already been dealt with ${provider}. ` +
              `Deal CID ${deal.dealCid}.`);
            alreadyDealt = true;
          }
        }
        if (alreadyDealt) {
          continue;
        }
        if (existingDeals.length >= existingRec.maxReplicas) {
          this.logger.warn(`This pieceCID ${carFile.pieceCid} has reached enough ` +
            `replica (${existingRec.maxReplicas}) planned by the request ${existingRec.id}.`);
          continue;
        }

        let dealCmd = '';
        try {
          dealCmd = await this.createDealCmd(useLotus, provider, replicationRequest, carFile);
        } catch (error) {
          this.logger.error(`Deal CMD generation failed`, error);
          continue;
        }

        this.logger.debug(dealCmd);
        let dealCid = 'unknown';
        let errorMsg = '';
        let state = 'proposed';
        let retryCount = 0;
        do {
          try {
            const cmdOut = await exec(dealCmd);
            this.logger.info(cmdOut.stdout);
            if (useLotus) {
              if (cmdOut.stdout.startsWith('baf')) {
                dealCid = cmdOut.stdout;
                break; // success, break from while loop
              } else {
                errorMsg = cmdOut.stdout + cmdOut.stderr;
                state = 'error';
              }
            } else {
              const match = boostResultUUIDMatcher.exec(cmdOut.stdout);
              if (match != null && match.length > 1) {
                dealCid = match[1];
                break; // success, break from while loop
              } else {
                errorMsg = cmdOut.stdout + cmdOut.stderr;
                state = 'error';
              }
            }
          } catch (error) {
            this.logger.error('Deal making failed', error);
            errorMsg = '' + error;
            state = 'error';
          }
          this.logger.info(`Waiting ${retryTimeout} ms to retry`);
          await new Promise(resolve => setTimeout(resolve, retryTimeout));
          retryTimeout *= 2; // expoential back off
          retryCount++;
        } while (retryCount < config.get<number>('deal_replication_worker.max_retry_count'));
        if (state === 'proposed') {
          dealsMadePerSP++;
          if (retryTimeout > config.get<number>('deal_replication_worker.min_retry_wait_ms')) {
            retryTimeout /= 2; // expoential back "on"
          }
        }
        await Datastore.DealStateModel.create({
          client: replicationRequest.client,
          provider: provider,
          dealCid: dealCid,
          dataCid: carFile.dataCid,
          pieceCid: carFile.pieceCid,
          expiration: 0,
          duration: replicationRequest.duration,
          price: replicationRequest.maxPrice, // unit is Fil per epoch per GB
          verified: replicationRequest.isVerfied,
          state: state,
          replicationRequestId: replicationRequest.id,
          datasetId: replicationRequest.datasetId,
          errorMessage: errorMsg
        });
      }
    }
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
