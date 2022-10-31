import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ReplicationRequest from '../common/model/ReplicationRequest';
import axios from 'axios';
import { create, all } from 'mathjs';
import GenerationRequest from '../common/model/GenerationRequest';
import cron, { ScheduledTask } from 'node-cron';
import fs from 'fs-extra';
import config from '../common/Config';
import { AbortSignal } from '../common/AbortSignal';
import { exec } from 'promisify-child-process';
import { sleep } from '../common/Util';
import { HeightFromCurrentTime } from '../common/ChainHeight';
import GenerateCsv from '../common/GenerateCsv';
import MetricEmitter from '../common/metrics/MetricEmitter';

const mathconfig = {};
const math = create(all, mathconfig);

export default class DealReplicationWorker extends BaseService {
  private readonly lotusCMD: string;
  private readonly boostCMD: string;
  // holds reference to all started crons to be updated
  private cronRefArray: Map<string, [schedule: string, taskRef: ScheduledTask]> = new Map<string, [string, ScheduledTask]>();

  public constructor () {
    super(Category.DealReplicationWorker);
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
    this.lotusCMD = config.get('deal_replication_worker.lotus_cli_cmd');
    this.boostCMD = config.get('deal_replication_worker.boost_cli_cmd');
  }

  public async start (): Promise<void> {
    if (!this.enabled) {
      this.logger.warn('Deal Replication Worker is not enabled. Exit now...');
    }

    await this.initialize();
    this.startHealthCheck();
    this.startPollWork();
  }

  private readonly PollInterval = 5000;
  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let workDone = false;
    try {
      workDone = await this.pollWork();
    } catch (err) {
      this.logger.error('Pool work error', err);
    }
    if (workDone) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
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

    for (const request2Check of activeCronWorks) {
      if (this.cronRefArray.has(request2Check.id)) {
        const [schedule, taskRef] = this.cronRefArray.get(request2Check.id)!;
        if (schedule !== request2Check.cronSchedule) {
          // cron schedule changed from update request
          taskRef.stop();
          this.cronRefArray.delete(request2Check.id);
          await Datastore.ReplicationRequestModel.findByIdAndUpdate(
            request2Check.id, { workerId: null }); // will be picked up again by the immediate pollReplicationWork
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
   *
   * @param storageProviders
   * @returns
   */
  public static generateProvidersList (storageProviders: string): Array<string> {
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
    if (cmdOut?.stdout?.toString()?.includes('/fil/storage/mk/1.2.0')) {
      useLotus = false;
      this.logger.debug(`SP ${provider} supports boost.`);
    } else if (cmdOut?.stdout?.toString()?.includes('/fil/storage/mk/1.1.0')) {
      this.logger.debug(`SP ${provider} supports lotus.`);
    } else {
      throw new Error(JSON.stringify(cmdOut));
    }
    return useLotus;
  }

  private static calculatePriceWithSize (price: number, pieceSize: number): string {
    let unit;
    if (price > 0) {
      unit = math.unit(price * (pieceSize || 0) / 1073741824, 'm');
    } else {
      unit = math.unit(0, 'm');
    }
    return math.format(unit.toNumber(), { notation: 'fixed' });
  }

  private static assembleDownloadUrl (urlPrefix: string, pieceCid?: string, filenameOverride?: string) {
    let downloadUrl = urlPrefix;
    if (!downloadUrl.endsWith('/')) {
      downloadUrl += '/';
    }
    let carFilename = pieceCid! + '.car';
    if (filenameOverride) {
      carFilename = filenameOverride;
      if (!filenameOverride.endsWith('.car')) {
        carFilename += '.car';
      }
    }
    downloadUrl += carFilename;
    return downloadUrl;
  }

  private async isUrlReachable (url: string): Promise<boolean> {
    try {
      await axios.head(url);
      return true;
    } catch (error) {
      this.logger.error(`This download URL ${url} is not reachable.`);
      return false;
    }
  }

  private async createDealCmd (
    useLotus: boolean,
    provider: string,
    replicationRequest: ReplicationRequest,
    carFile: GenerationRequest,
    startEpoch: number): Promise<string> {
    if (useLotus) {
      if (replicationRequest.isOffline) {
        const priceInFilWithSize = DealReplicationWorker.calculatePriceWithSize(replicationRequest.maxPrice, carFile.pieceSize!);
        const unpaddedSize = carFile.pieceSize! * 127 / 128;
        const manualStateless = replicationRequest.maxPrice > 0 ? '' : '--manual-stateless-deal';// only zero priced deal support manual stateless deal
        // TODO consider implement something similar to this to get rid of collateral below minimum error when proposing with lotus
        // https://github.com/filecoin-project/boost/blob/d561bbf72e40c0b7b19f359ae23a8c0d1afd910d/cmd/boost/deal_cmd.go#L209
        return `${this.lotusCMD} client deal --manual-piece-cid=${carFile.pieceCid} --manual-piece-size=${unpaddedSize} ` +
          `${manualStateless} --from=${replicationRequest.client} --verified-deal=${replicationRequest.isVerfied} --start-epoch=${startEpoch} ` +
          `${carFile.dataCid} ${provider} ${priceInFilWithSize} ${replicationRequest.duration}`;
      } else {
        throw new Error(`Deal making failed. SP ${provider} only supports lotus and for lotus we only support offline deals.`);
      }
    } else {
      // determine car download link
      const downloadUrl = DealReplicationWorker.assembleDownloadUrl(replicationRequest.urlPrefix, carFile.pieceCid, carFile.filenameOverride);
      // check if download URL is reachable if necessary
      if (!replicationRequest.isOffline && !(await this.isUrlReachable(downloadUrl))) {
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
    let isComplete = false;
    const numberOfSPs = DealReplicationWorker.generateProvidersList(request2Check.storageProviders).length;
    if (!request2Check.cronSchedule) {
      isComplete = true; // Non-cron, perform quick check
    } else {
      const maxNumberOfDeals = request2Check.cronSchedule ? request2Check.cronMaxDeals : request2Check.maxNumberOfDeals;
      const actualDealsCount = await Datastore.DealStateModel.count({
        replicationRequestId: request2Check.id,
        state: {
          $nin: [
            'slashed', 'error', 'expired', 'proposal_expired'
          ]
        }
      });
      this.logger.debug(`checkAndMarkCompletion ${request2Check.id} max: ${maxNumberOfDeals} actual: ${actualDealsCount}`);
      // TODO Below two conditions are not a precise way to determine if a cron job is complete
      if (maxNumberOfDeals != null && maxNumberOfDeals > 0 && actualDealsCount >= (maxNumberOfDeals! * numberOfSPs)) {
        this.logger.debug(`Actual deals over limit`);
        isComplete = true;
      } else if (actualDealsCount >= (carCount * Math.min(numberOfSPs, request2Check.maxReplicas))) {
        this.logger.debug(`Actual deals under limit but no more cars available`);
        isComplete = true;
      }
    }
    if (isComplete) {
      await Datastore.ReplicationRequestModel.findOneAndUpdate({
        _id: request2Check.id
      }, {
        status: 'completed',
        workerId: null
      });
      this.stopCronIfExist(request2Check.id);
      if (request2Check.csvOutputDir) {
        const csvMsg = await GenerateCsv.generate(request2Check.id, request2Check.csvOutputDir);
        this.logger.info(`Mark as complete. ${csvMsg}`);
      } else {
        this.logger.info(`Mark as complete. To print CSV: singularity repl csv ${request2Check.id} /tmp`);
      }
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
  /* istanbul ignore next */
  private async replicate (replicationRequest: ReplicationRequest): Promise<void> {
    this.logger.info(`Start replication ${replicationRequest.id}`);
    let breakOuter = false; // set this to true will terminate all concurrent deal making thread
    let fileList: Array<string> = [];
    if (replicationRequest.fileListPath) {
      try {
        const fileListStr = await fs.readFile(replicationRequest.fileListPath, 'utf-8');
        fileList = fileListStr.split(/\r?\n/);
        this.logger.info(`Replication is limited to content in ${replicationRequest.fileListPath}, found ${fileList.length} lines.`);
      } catch (error) {
        breakOuter = true;
        this.logger.error(`Read fileListPath failed from ${replicationRequest.fileListPath}`, error);
      }
    }
    try {
      const providers = DealReplicationWorker.generateProvidersList(replicationRequest.storageProviders);
      const makeDealAll = providers.map(async (provider) => {
        // Find cars that are finished generation
        const cars = await Datastore.GenerationRequestModel.find({
          datasetId: replicationRequest.datasetId,
          status: 'completed'
        })
          .sort({
            pieceCid: 1
          });
        if (breakOuter) {
          this.stopCronIfExist(replicationRequest.id);
          return;
        }
        let useLotus = true;
        try {
          useLotus = await this.isUsingLotus(provider);
        } catch (error) {
          this.logger.error(`SP ${provider} unknown output from libp2p. Assume lotus.`, error);
        }

        let dealsMadePerSP = 0;
        // in the event of daemon restart while actively sending, need to query how many have been made
        if (!replicationRequest.cronSchedule) {
          dealsMadePerSP = await Datastore.DealStateModel.count({
            replicationRequestId: replicationRequest.id,
            provider: provider,
            state: {
              $in: [
                'proposed', 'published', 'active'
              ]
            }
          });
          if (dealsMadePerSP > 0) {
            this.logger.warn(`${provider} already been dealt with ${dealsMadePerSP} deals.`);
          }
        }
        let retryWait = config.get<number>('deal_replication_worker.min_retry_wait_ms'); // 30s, 60s, 120s ...
        for (let j = 0; j < cars.length; j++) {
          const carFile = cars[j];
          // check if file belongs to fileList
          if (fileList.length > 0 && carFile.pieceCid) {
            let matched = false;
            for (let k = 0; k < fileList.length; k++) {
              if (fileList[k].endsWith(carFile.pieceCid + '.car') || fileList[k].endsWith(carFile.dataCid + '.car')) {
                matched = true;
                break;
              }
            }
            if (!matched) {
              this.logger.debug(`File ${carFile.pieceCid} is not on the list`);
              continue;
            }
          }

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
                  'proposed', 'published', 'active'
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
            if (existingRec.cronSchedule) {
              breakOuter = await this.checkAndMarkCompletion(existingRec, cars.length);
            }
            return;
          }

          if (!existingRec.isForced) {
            // check if the car has already dealt or have enough replica
            const existingDeals = await Datastore.DealStateModel.find({
              // due to unknown bug, DealState can have mismatch piece/data CID
              $or: [{ pieceCid: carFile.pieceCid }, { dataCid: carFile.dataCid }],
              state: { $nin: ['slashed', 'error', 'expired', 'proposal_expired'] }
            });
            let alreadyDealt = false;
            for (let k = 0; k < existingDeals.length; k++) {
              const deal = existingDeals[k];
              if (deal.pieceCid !== carFile.pieceCid) {
                this.logger.warn(`This dealCID ${deal.dealCid} has mismatch pieceCID ${deal.pieceCid}. It should have been ${carFile.pieceCid}.`);
              }
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
              this.logger.debug(`This pieceCID ${carFile.pieceCid} has reached enough ` +
                `replica (${existingRec.maxReplicas}) planned by the request ${existingRec.id}.`);
              continue; // go to next file
            }
          }

          const startDelay = replicationRequest.startDelay ? replicationRequest.startDelay : 20160;
          const currentHeight = HeightFromCurrentTime();
          const startEpoch = startDelay + currentHeight + 60; // 30 min buffer time
          this.logger.debug(`Calculated start epoch startDelay: ${startDelay} + currentHeight: ${currentHeight} + 60 = ${startEpoch}`);
          let dealCmd = '';
          try {
            dealCmd = await this.createDealCmd(useLotus, provider, replicationRequest, carFile, startEpoch);
          } catch (error) {
            this.logger.error(`Deal CMD generation failed`, error);
            return;
          }
          const {
            dealCid,
            errorMsg,
            state,
            retryTimeout
          } = await this.makeDeal(dealCmd, carFile.pieceCid!, provider, dealsMadePerSP, useLotus, retryWait);
          retryWait = retryTimeout;
          if (state === 'proposed') {
            dealsMadePerSP++;
            if (retryWait > config.get<number>('deal_replication_worker.min_retry_wait_ms')) {
              retryWait = retryWait /= 2; // expoential back "on"
            }
            await MetricEmitter.Instance().emit({
              type: 'deal_proposed',
              values: {
                protocol: useLotus ? 'lotus' : 'boost',
                pieceCid: carFile.pieceCid,
                dataCid: carFile.dataCid,
                pieceSize: carFile.pieceSize,
                carSize: carFile.carSize,
                provider: provider,
                client: replicationRequest.client,
                verified: replicationRequest.isVerfied,
                duration: replicationRequest.duration,
                price: replicationRequest.maxPrice,
                proposalCid: dealCid
              }
            });
          } else {
            await MetricEmitter.Instance().emit({
              type: 'deal_proposal_failed',
              values: {
                protocol: useLotus ? 'lotus' : 'boost',
                pieceCid: carFile.pieceCid,
                dataCid: carFile.dataCid,
                pieceSize: carFile.pieceSize,
                carSize: carFile.carSize,
                provider: provider,
                client: replicationRequest.client,
                verified: replicationRequest.isVerfied,
                duration: replicationRequest.duration,
                price: replicationRequest.maxPrice,
                errorMsg: errorMsg
              }
            });
          }

          await Datastore.DealStateModel.create({
            client: replicationRequest.client,
            provider: provider,
            dealCid: dealCid,
            dataCid: carFile.dataCid,
            pieceCid: carFile.pieceCid,
            pieceSize: carFile.pieceSize,
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
      });
      await Promise.all(makeDealAll);
      this.logger.debug(`Finished all files in the dataset. Checking completion.`);
      const reRead = await Datastore.ReplicationRequestModel.findById(replicationRequest.id);
      const carCount = await Datastore.GenerationRequestModel.count({
        datasetId: reRead?.datasetId,
        status: 'completed'
      });
      await this.checkAndMarkCompletion(reRead!, carCount);
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

  private async makeDeal (
    dealCmd: string,
    pieceCid: string,
    provider: string,
    dealsMadePerSP: number,
    useLotus: boolean,
    retryTimeout: number) {
    const boostResultUUIDMatcher = /deal uuid: (\S+)/;
    this.logger.debug(dealCmd);
    let dealCid = 'unknown';
    let errorMsg = '';
    let state = 'proposed';
    let retryCount = 0;
    do {
      try {
        const cmdOut = await exec(dealCmd);
        this.logger.info(`Dealt ${pieceCid} with ${provider} (#${dealsMadePerSP}), output: ${cmdOut.stdout}`);
        if (useLotus) {
          if (cmdOut?.stdout?.toString().startsWith('baf')) {
            dealCid = cmdOut?.stdout?.toString().trim(); // remove trailing line break
            // If there was a retry, these values could be in error state, need reset
            errorMsg = '';
            state = 'proposed';
            break; // success, break from while loop
          } else {
            errorMsg = (cmdOut?.stdout?.toString() ?? '') + (cmdOut?.stderr?.toString() ?? '');
            state = 'error';
          }
        } else {
          const match = boostResultUUIDMatcher.exec(cmdOut?.stdout?.toString() ?? '');
          if (match != null && match.length > 1) {
            dealCid = match[1];
            // If there was a retry, these values could be in error state, need reset
            errorMsg = '';
            state = 'proposed';
            break; // success, break from while loop
          } else {
            errorMsg = (cmdOut?.stdout?.toString() ?? '') + (cmdOut?.stderr?.toString() ?? '');
            state = 'error';
          }
        }
      } catch (error) {
        this.logger.error('Deal making failed', error);
        errorMsg = '' + error;
        state = 'error';
      }
      this.logger.info(`Waiting ${retryTimeout} ms to retry`);
      await sleep(retryTimeout);
      if (errorMsg.includes('proposed provider collateral below minimum')) {
        this.logger.warn(`Keep retry on this error without expoential back off. These can usually resolve itself within timely manner.`);
      } else {
        retryTimeout *= 2; // expoential back off
      }
      retryCount++;
    } while (retryCount < config.get<number>('deal_replication_worker.max_retry_count'));
    return {
      dealCid,
      errorMsg,
      state,
      retryTimeout
    };
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
      {},
      {
        upsert: true
      }
    );
  }
}
