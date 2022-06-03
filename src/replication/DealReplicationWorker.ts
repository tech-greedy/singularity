/* eslint @typescript-eslint/no-var-requires: "off" */
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import ReplicationRequest from '../common/model/ReplicationRequest';
import config from 'config';
import { create, all } from 'mathjs';
const mathconfig = {};
const math = create(all, mathconfig);
const exec: any = require('await-exec');// no TS support

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
    miners.forEach(async miner => {
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
          return;
        }
      } catch (error) {
        this.logger.error(`SP ${miner} unknown output from libp2p. Give up on this SP.`, error);
        return;
      }

      // Find cars that are finished generation
      const cars = await Datastore.GenerationRequestModel.find({
        datasetId: model.datasetId,
        status: 'completed'
      });
      cars.forEach(async carFile => {
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
        downloadUrl += carFile.dataCid + '.car';
        // TODO do a HEAD on the link to preliminary validate, if invalid, throw exception and mark state

        // calculate duration
        const durationInEpoch = 2880 * model.duration;

        let dealCmd = '';
        if (useLotus) {
          if (model.isOffline) {
            const unpaddedSize = carFile.pieceSize! * 127 / 128;
            dealCmd = `${lotusCMD} client deal --manual-piece-cid=${carFile.pieceCid} --manual-piece-size=${unpaddedSize} ` +
              `--manual-stateless-deal --from=${model.client} --verified-deal=${model.isVerfied} ` +
              `${carFile.dataCid} ${miner} ${priceInFil.toNumber()} ${durationInEpoch}`;
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
            `--verified=${model.isVerfied} --wallet=${model.client} --duration=${durationInEpoch}`;
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
          await new Promise(resolve => setTimeout(resolve, 30000));
          this.logger.info('Waiting 30 seconds to retry');
          retryCount++;
        } while (retryCount < 3);
        await Datastore.DealStateModel.create({
          client: model.client,
          provider: miner,
          dealCid: dealCid,
          dataCid: carFile.dataCid,
          pieceCid: carFile.pieceCid,
          expiration: 0,
          duration: durationInEpoch,
          price: priceInFil.toNumber(), // unit is Fil
          verified: model.isVerfied,
          state: state,
          replicationRequestId: model.id,
          datasetId: model.datasetId,
          errorMessage: errorMsg
        });
      });
    });
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