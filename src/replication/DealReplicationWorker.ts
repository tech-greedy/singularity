/* eslint @typescript-eslint/no-var-requires: "off" */
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ReplicationRequest from '../common/model/ReplicationRequest';
import axios from 'axios';
import { create, all, Unit } from 'mathjs';
import GenerationRequest from '../common/model/GenerationRequest';
import cron, { ScheduledTask } from 'node-cron';
import config from '../common/Config';
const mathconfig = {};
const math = create(all, mathconfig);
const exec: any = require('await-exec');// no TS support

export default class DealReplicationWorker extends BaseService {
  private readonly workerId: string;
  private readonly lotusCMD: string;
  private readonly boostCMD: string;
  // holds reference to all started crons to be updated
  private cronRefArray: Map<string, [schedule: string, taskRef: ScheduledTask]> = new Map<string, [string, ScheduledTask]>();

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

  private async startPollWork (): Promise<void> {
    try {
      await this.pollWork();
    } catch (err) {
      this.logger.error('Pool work error', err);
    }
    setTimeout(this.startPollWork, this.PollInterval);
  }

  private async pollWork (): Promise<boolean> {
    this.logger.debug(`${this.workerId} - Polling for work`);
    await this.checkCronChange();
    const hasDoneWork = await this.pollReplicationWork();
    return hasDoneWork;
  }

  private async checkCronChange (): Promise<void> {
    const activeCronWorks = await Datastore.ReplicationRequestModel.find({
      cronSchedule: { $ne: null },
      workerId: { $ne: null },
      status: 'active'
    });

    for (let i = 0; i < activeCronWorks.length; i++) {
      const request2Check = activeCronWorks[i];
      if (this.cronRefArray.has(request2Check.id)) {
        const [schedule, taskRef] = this.cronRefArray.get(request2Check.id)!;
        if (schedule !== request2Check.cronSchedule) {
          // cron schedule changed from update request
          taskRef.stop();
          this.cronRefArray.delete(request2Check.id);
          await Datastore.ReplicationRequestModel.findOneAndUpdate({
            _id: request2Check.id
          }, {
            workerId: null
          }); // will be picked up again by the immediate pollReplicationWork
          this.logger.info(`Cron changed, restarting schedule. (${request2Check.id})`);
        }
      }
    }
  }

  private async pollReplicationWork (): Promise<boolean> {
    const newReplicationWork = await Datastore.ReplicationRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId
    });
    if (newReplicationWork) {
      this.logger.info(`${this.workerId} - Received a new request - id ${newReplicationWork.id} dataset: ${newReplicationWork.datasetId}`);
      if (newReplicationWork.cronSchedule) {
        this.logger.debug(`Schedule and start cron (${newReplicationWork.cronSchedule})`);
        this.cronRefArray.set(newReplicationWork.id, [
          newReplicationWork.cronSchedule,
          cron.schedule(newReplicationWork.cronSchedule, async () => {
            this.logger.debug(`Cron triggered (${newReplicationWork.cronSchedule}) - id ${newReplicationWork.id}`);
            await this.replicate(newReplicationWork);
          })]);
      } else {
        this.logger.debug(`No cron, execute immediately`);
        this.replicate(newReplicationWork); // no await so that this can return quickly, enabling parallel polling
      }
    }

    return newReplicationWork != null;
  }

  /**
   * Create providers list by storageProviders. Currently it is just a list of providers separated by comma.
   * TODO marking this function as async pending future Pando integration
   *
   * @param storageProviders
   * @returns
   */
  private static async generateProvidersList (storageProviders: string): Promise<Array<string>> {
    return storageProviders.split(',');
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
    let carFilename = carFile.pieceCid + '.car';
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

  public static async getCurrentChainHeight (lotusCmd: string): Promise<number> {
    const cmdOut = await exec(`${lotusCmd} chain list --count 1 --format "<height>"`);
    return parseInt(cmdOut.stdout);
  }

  private async createDealCmd (useLotus: boolean, provider: string, replicationRequest: ReplicationRequest,
    carFile: GenerationRequest, startEpoch: number): Promise<string> {
    if (useLotus) {
      if (replicationRequest.isOffline) {
        const priceInFilWithSize = math.format(DealReplicationWorker.calculatePriceWithSize(replicationRequest.maxPrice, carFile.pieceSize!).toNumber(),
          { notation: 'fixed' });
        const unpaddedSize = carFile.pieceSize! * 127 / 128;
        const manualStateless = replicationRequest.maxPrice > 0 ? '' : '--manual-stateless-deal';// only zero priced deal support manual stateless deal
        return `${this.lotusCMD} client deal --manual-piece-cid=${carFile.pieceCid} --manual-piece-size=${unpaddedSize} ` +
          `${manualStateless} --from=${replicationRequest.client} --verified-deal=${replicationRequest.isVerfied} --start-epoch=${startEpoch} ` +
          `${carFile.dataCid} ${provider} ${priceInFilWithSize} ${replicationRequest.duration}`;
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
      const priceInAttoWithoutSize = math.format(math.unit(replicationRequest.maxPrice, 'm').toNumber('am'), { notation: 'fixed' });
      let propose = `deal --http-url=${downloadUrl}`;
      if (replicationRequest.isOffline) {
        propose = `offline-deal`;
      }
      return `${this.boostCMD} ${propose} --provider=${provider}  --commp=${carFile.pieceCid} --car-size=${carFile.carSize} ` +
        `--piece-size=${carFile.pieceSize} --payload-cid=${carFile.dataCid} --storage-price=${priceInAttoWithoutSize} --start-epoch=${startEpoch} ` +
        `--verified=${replicationRequest.isVerfied} --wallet=${replicationRequest.client} --duration=${replicationRequest.duration}`;
    }
  }

  private stopCronIfExist (id: string): void {
    if (this.cronRefArray.has(id)) {
      const [schedule, taskRef] = this.cronRefArray.get(id)!;
      taskRef.stop();
      this.cronRefArray.delete(id);
      this.logger.debug(`Stopped cron (${schedule}) of ${id}`);
    }
  }

  private async checkAndMarkCompletion (request2Check: ReplicationRequest, carCount: number): Promise<boolean> {
    const maxNumberOfDeals = request2Check.cronSchedule ? request2Check.cronMaxDeals : request2Check.maxNumberOfDeals;
    const numberOfSPs = (await DealReplicationWorker.generateProvidersList(request2Check.storageProviders)).length;
    const actualDealsCount = await Datastore.DealStateModel.count({
      replicationRequestId: request2Check.id,
      state: {
        $nin: [
          'slashed', 'error', 'expired', 'proposal_expired'
        ]
      }
    });
    this.logger.debug(`checkAndMarkCompletion ${request2Check.id} max: ${maxNumberOfDeals} actual: ${actualDealsCount}`);
    let isComplete = false;
    if (maxNumberOfDeals != null && maxNumberOfDeals > 0 && actualDealsCount >= (maxNumberOfDeals! * numberOfSPs)) {
      this.logger.debug(`Actual deals over limit`);
      isComplete = true;
    } else if (actualDealsCount >= (carCount * Math.min(numberOfSPs, request2Check.maxReplicas))) {
      this.logger.debug(`Actual deals under limit but no more cars available`);
      isComplete = true;
    }
    if (isComplete) {
      this.logger.debug(`Mark as complete`);
      await Datastore.ReplicationRequestModel.findOneAndUpdate({
        _id: request2Check.id
      }, {
        status: 'completed',
        workerId: null
      });
      this.stopCronIfExist(request2Check.id);
      return true;
    } else {
      this.logger.debug(`Not yet complete`);
      return false;
    }
  }

  /**
   * Main function of deal making
   * @param replicationRequest
   */
  private async replicate (replicationRequest: ReplicationRequest): Promise<void> {
    this.logger.debug(`Start replication ${replicationRequest.id}`);
    let breakOuter = false; // set this to true will terminate all concurrent deal making thread
    const boostResultUUIDMatcher = /deal uuid: (\S+)/;
    try {
      const providers = await DealReplicationWorker.generateProvidersList(replicationRequest.storageProviders);
      const makeDealAll = providers.map(async (provider) => {
        if (breakOuter) {
          this.stopCronIfExist(replicationRequest.id);
          return;
        }
        let useLotus = true;
        try {
          useLotus = await this.isUsingLotus(provider);
        } catch (error) {
          this.logger.error(`SP ${provider} unknown output from libp2p. Give up on this SP.`, error);
          return;
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
            return;
          }
          if (existingRec.status !== 'active') {
            this.logger.warn(`This replication request ${existingRec.id} has become non-active: ${existingRec.status}.`);
            breakOuter = true;
            return;
          }

          // check if reached max pending deal during a cron
          if (config.get('deal_tracking_service.enabled') && existingRec.cronSchedule && existingRec.cronMaxPendingDeals && existingRec.cronMaxPendingDeals > 0) {
            const pendingDeals = await Datastore.DealStateModel.count({
              provider: provider,
              state: {
                $in: [
                  'proposed', 'published'
                ]
              }
            });
            if (pendingDeals >= existingRec.cronMaxPendingDeals) {
              this.logger.warn(`This SP ${provider} has reached max pending deals allowed (${existingRec.cronMaxPendingDeals}) by request ${existingRec.id}.`);
              return;
            } else {
              this.logger.debug(`This SP ${provider} has ${pendingDeals} / ${existingRec.cronMaxPendingDeals} pending deals.`);
            }
          }

          // check if reached max deals needed to be sent
          if (existingRec.maxNumberOfDeals > 0 && dealsMadePerSP >= existingRec.maxNumberOfDeals) {
            this.logger.warn(`This SP ${provider} has made max deals planned (${existingRec.maxNumberOfDeals}) by the request ${existingRec.id}.`);
            const shouldStopAll = await this.checkAndMarkCompletion(existingRec, cars.length);
            if (shouldStopAll) {
              breakOuter = true;
            }
            return;
          }

          // check if the car has already dealt or have enough replica
          const existingDeals = await Datastore.DealStateModel.find({
            pieceCid: carFile.pieceCid,
            state: { $nin: ['slashed', 'error', 'expired', 'proposal_expired'] }
          });
          let alreadyDealt = false;
          for (let k = 0; k < existingDeals.length; k++) {
            const deal = existingDeals[k];
            if (deal.provider === provider) {
              this.logger.debug(`This pieceCID ${carFile.pieceCid} has already been dealt with ${provider}. ` +
                `Deal CID ${deal.dealCid}. Moving on to next file.`);
              alreadyDealt = true;
            }
          }
          if (alreadyDealt) {
            continue; // go to next file
          }
          if (existingDeals.length >= existingRec.maxReplicas) {
            this.logger.warn(`This pieceCID ${carFile.pieceCid} has reached enough ` +
              `replica (${existingRec.maxReplicas}) planned by the request ${existingRec.id}.`);
            continue; // go to next file
          }

          const startDelay = replicationRequest.startDelay ? replicationRequest.startDelay : 20160;
          const currentHeight = await DealReplicationWorker.getCurrentChainHeight(this.lotusCMD);
          const startEpoch = startDelay + currentHeight;
          this.logger.debug(`Calculated start epoch startDelay: ${startDelay} + currentHeight: ${currentHeight} = ${startEpoch}`);
          let dealCmd = '';
          try {
            dealCmd = await this.createDealCmd(useLotus, provider, replicationRequest, carFile, startEpoch);
          } catch (error) {
            this.logger.error(`Deal CMD generation failed`, error);
            return;
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
                  dealCid = cmdOut.stdout.trim(); // remove trailing line break
                  // If there was a retry, these values could be in error state, need reset
                  errorMsg = '';
                  state = 'proposed';
                  break; // success, break from while loop
                } else {
                  errorMsg = cmdOut.stdout + cmdOut.stderr;
                  state = 'error';
                }
              } else {
                const match = boostResultUUIDMatcher.exec(cmdOut.stdout);
                if (match != null && match.length > 1) {
                  dealCid = match[1];
                  // If there was a retry, these values could be in error state, need reset
                  errorMsg = '';
                  state = 'proposed';
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
            startEpoch: startEpoch,
            expiration: startEpoch + replicationRequest.duration,
            duration: replicationRequest.duration,
            price: replicationRequest.maxPrice, // unit is Fil per epoch per GB
            verified: replicationRequest.isVerfied,
            state: state,
            replicationRequestId: replicationRequest.id,
            datasetId: replicationRequest.datasetId,
            errorMessage: errorMsg
          });
        }
        // cron schedule could change from outside
        this.logger.debug(`Finished all files in the dataset. Checking completion.`);
        const reRead = await Datastore.ReplicationRequestModel.findById(replicationRequest.id);
        await this.checkAndMarkCompletion(reRead!, cars.length);
      });
      await Promise.all(makeDealAll);
    } catch (err) {
      this.stopCronIfExist(replicationRequest.id);
      if (err instanceof Error) {
        this.logger.error(`${this.workerId} - Encountered an error - ${err.message}`);
        await Datastore.ReplicationRequestModel.findByIdAndUpdate(replicationRequest.id, {
          status: 'error',
          workerId: null,
          errorMessage: err.message
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
