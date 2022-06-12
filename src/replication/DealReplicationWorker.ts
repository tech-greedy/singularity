/* eslint @typescript-eslint/no-var-requires: "off" */
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ReplicationRequest from '../common/model/ReplicationRequest';
import config from 'config';
import axios from 'axios';
import { create, all } from 'mathjs';
const mathconfig = {};
const math = create(all, mathconfig);
const exec: any = require('await-exec');// no TS support
const ObjectsToCsv: any = require('objects-to-csv');// no TS support

export default class DealReplicationWorker extends BaseService {
  private readonly workerId: string;

  public constructor () {
    super(Category.DealReplicationWorker);
    this.workerId = randomUUID();
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
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
      // Create a deal tracking request if not exist
      await Datastore.DealTrackingStateModel.updateOne({
        stateType: 'client',
        stateKey: newReplicationWork.client
      }, {
        $setOnInsert: {
          stateType: 'client',
          stateKey: newReplicationWork.client,
          stateValue: 'track'
        }
      }, {
        upsert: true
      });
      await Datastore.ReplicationRequestModel.findByIdAndUpdate(newReplicationWork.id, { status: 'completed' });
    }

    return newReplicationWork != null;
  }

  /**
   * Main function of deal making
   * @param model
   */
  private async replicate (model: ReplicationRequest): Promise<void> {
    const miners = model.criteria.split(',');
    const lotusCMD = config.get('deal_replication_worker.lotus_cli_cmd');
    const boostCMD = config.get('deal_replication_worker.boost_cli_cmd');
    const boostResultUUIDMatcher = /deal uuid: (\S+)/;
    let breakOuter = false;
    for (let i = 0; i < miners.length; i++) {
      if (breakOuter) {
        break;
      }
      const miner = miners[i];
      let useLotus = true;
      // use boost libp2p command to check whether miner supports boost
      const versionQueryCmd = `${boostCMD} provider libp2p-info ${miner}`;
      try {
        const cmdOut = await exec(versionQueryCmd);
        if (cmdOut.stdout.includes('/fil/storage/mk/1.2.0')) {
          useLotus = false;
          this.logger.info(`SP ${miner} supports boost.`);
        } else if (cmdOut.stdout.includes('/fil/storage/mk/1.1.0')) {
          this.logger.info(`SP ${miner} supports lotus.`);
        } else {
          this.logger.error(`SP ${miner} unknown output from libp2p. Give up on this SP.`, cmdOut.stdout, cmdOut.stderr);
          continue;
        }
      } catch (error) {
        this.logger.error(`SP ${miner} unknown output from libp2p. Give up on this SP.`, error);
        continue;
      }

      // Find cars that are finished generation
      const cars = await Datastore.GenerationRequestModel.find({
        datasetId: model.datasetId,
        status: 'completed'
      });

      let dealsMadePerSP = 0;
      let retryTimeout = config.get<number>('min_retry_wait_ms'); // 30s, 60s, 120s ...
      const csvArray = [];
      for (let j = 0; j < cars.length; j++) {
        const carFile = cars[j];

        // check if the replication request has been paused
        const existingRec = await Datastore.ReplicationRequestModel.findById(model.id);
        if (!existingRec) {
          this.logger.error(`This replication request ${model.id} no longer exist.`);
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
          this.logger.warn(`This SP ${miner} has made max deals planned (${existingRec.maxNumberOfDeals}) by the request ${existingRec.id}.`);
          break;
        }

        // check if the car has already dealt or have enough replica
        const existingDeals = await Datastore.DealStateModel.find({
          state: { $nin: ['slashed', 'error'] },
          pieceCid: carFile.pieceCid
        });
        let alreadyDealt = false;
        for (let k = 0; k < existingDeals.length; k++) {
          const deal = existingDeals[k];
          if (deal.provider === miner) {
            this.logger.warn(`This pieceCID ${carFile.pieceCid} has already been dealt with ${miner}. ` +
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

        // determine price in Fil (for lotus) and AttoFil (for boost)
        let priceInFil = math.unit(0, 'm');
        if (model.maxPrice > 0) {
          priceInFil = math.unit(model.maxPrice * (carFile.pieceSize || 0) / 1073741824, 'm');
        }
        const priceInAtto = priceInFil.toNumber('am');

        // determine car download link TODO: assemble URL with builtin hosting service
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

        // do a HEAD on the link to preliminary validate, if invalid, throw exception and mark state
        if (!useLotus && !model.isOffline) {
          try {
            await axios.head(downloadUrl);
          } catch (error) {
            this.logger.error(`This download URL ${downloadUrl} is not reachable.`);
            continue;
          }
        }

        let dealCmd = '';
        if (useLotus) {
          if (model.isOffline) {
            const unpaddedSize = carFile.pieceSize! * 127 / 128;
            dealCmd = `${lotusCMD} client deal --manual-piece-cid=${carFile.pieceCid} --manual-piece-size=${unpaddedSize} ` +
              `--manual-stateless-deal --from=${model.client} --verified-deal=${model.isVerfied} ` +
              `${carFile.dataCid} ${miner} ${priceInFil.toNumber()} ${model.duration}`;
          } else {
            this.logger.error(`Deal making failed. SP ${miner} only supports lotus and for lotus we only support offline deals.`);
            return;
          }
        } else {
          let propose = `deal --http-url=${downloadUrl}`;
          if (model.isOffline) {
            propose = `offline-deal`;
          }
          dealCmd = `${boostCMD} ${propose} --provider=${miner}  --commp=${carFile.pieceCid} --car-size=${carFile.carSize} ` +
            `--piece-size=${carFile.pieceSize} --payload-cid=${carFile.dataCid} --storage-price-per-epoch=${priceInAtto} ` +
            `--verified=${model.isVerfied} --wallet=${model.client} --duration=${model.duration}`;
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
        } while (retryCount < config.get<number>('max_retry_count'));
        if (state === 'proposed') {
          dealsMadePerSP++;
          if (retryTimeout > config.get<number>('min_retry_wait_ms')) {
            retryTimeout /= 2; // expoential back "on"
          }
        }
        await Datastore.DealStateModel.create({
          client: model.client,
          provider: miner,
          dealCid: dealCid,
          dataCid: carFile.dataCid,
          pieceCid: carFile.pieceCid,
          expiration: 0,
          duration: model.duration,
          price: priceInFil.toNumber(), // unit is Fil
          verified: model.isVerfied,
          state: state,
          replicationRequestId: model.id,
          datasetId: model.datasetId,
          errorMessage: errorMsg
        });
        if (dealCid !== 'unknown') {
          csvArray.push({
            provider: miner,
            deal_cid: dealCid,
            filename: carFilename,
            piece_cid: carFile.pieceCid,
            url: downloadUrl
          });
        }
      }
      if (csvArray.length > 0) {
        const csv = new ObjectsToCsv(csvArray);
        const csvFilename = `/tmp/${miner}_${model.id}.csv`;
        await csv.toDisk(csvFilename, { append: true });
        this.logger.info(`CSV written to ${csvFilename}`);
      } else {
        this.logger.warn(`No deal made. Skip create CSV.`);
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
