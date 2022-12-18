import BaseService from '../common/BaseService';
import { Category } from '../common/Logger';
import Datastore from '../common/Datastore';
import axios, { AxiosRequestHeaders } from 'axios';
import config from '../common/Config';
import { HeightFromCurrentTime } from '../common/ChainHeight';
import MetricEmitter from '../common/metrics/MetricEmitter';

export default class DealTrackingService extends BaseService {
  public constructor () {
    super(Category.DealTrackingService);
    this.startDealTracking = this.startDealTracking.bind(this);
  }

  public async start (): Promise<void> {
    if (!this.enabled) {
      this.logger.warn('Service is not enabled. Exit now...');
    }

    await this.initialize();
    this.startDealTracking();
  }

  private async startDealTracking (): Promise<void> {
    await this.dealTracking();
    setTimeout(this.startDealTracking, config.getOrDefault<number>('interval_ms.lotus_api', 3_600_000));
  }

  private async dealTracking (): Promise<void> {
    this.logger.info('Start update deal tracking');
    const clientStates = await Datastore.DealTrackingStateModel.find({ stateType: 'client', stateValue: 'track' });
    for (const clientState of clientStates) {
      const client = clientState.stateKey;
      const lastDeal = await Datastore.DealStateModel.find({ client, "dealId": { $ne: null} }).sort({ dealId: -1 }).limit(1);
      try {
        await this.insertDealFromFilscan(client, lastDeal.length > 0 ? lastDeal[0].dealId! : 16000000);
      } catch (error) {
        this.logger.error('Encountered an error when importing deals from filescan', error);
      }
      try {
        await this.updateDealFromLotus(client);
        // only clean up expired deals when updateDealFromLotus update success
        await this.markExpiredDeals(client);
      } catch (error) {
        this.logger.error('Encountered an error when updating deals from lotus', error);
      }
    }
  }

  private static readonly FilscanPagination = 25;

  /**
   * Read from filscan api for PublishStorageDeal status
   *
   * @param client
   * @param lastDeal
   */
  private async insertDealFromFilscan (client: string, lastDeal: number): Promise<void> {
    this.logger.debug('updating deals from filscan', { client, lastDeal });
    let url = 'https://api.filscan.io:8700/rpc/v1';
    if (client.startsWith('t')) {
      url = 'https://calibration.filscan.io:8700/rpc/v1';
    }

    /**
     * Find the corresponding f012345 client ID of the client address
     */
    const lotusApi = config.get<string>('deal_tracking_service.lotus_api');
    const lotusToken = config.get<string>('deal_tracking_service.lotus_token');
    const headers: AxiosRequestHeaders = {};
    if (lotusToken !== '') {
      headers['Authorization'] = `Bearer ${lotusToken}`;
    }
    let clientId = null;
    let response = await axios.post(lotusApi, {
      id: 1,
      jsonrpc: '2.0',
      method: 'Filecoin.StateLookupID',
      params: [client, null]
    }, { headers });
    if (response.data.result && (response.data.result.startsWith('f0'))) {
      clientId = response.data.result;
    } else {
      this.logger.warn(`Cannot obtain client ID of ${client}`);
      return;
    }

    /**
     * We don't trust filscan's data, can only use the first deal id
     * as reference to see how many need to track
     */

    response = await axios.post(url, {
      id: 1,
      jsonrpc: '2.0',
      params: [client, 0, DealTrackingService.FilscanPagination],
      method: 'filscan.GetMarketDeal'
    }, {
      headers: {
        'content-type': 'application/json'
      }
    });

    const maxNumberOfDealsToTrack = response.data['result']['total'] | 0;
    let latestDealIdFromFilscan = 0;
    if (Array.isArray(response.data['result']['deals'])) {
      latestDealIdFromFilscan = response.data['result']['deals'][0]['dealid']; // could be wrong
    }
    if (maxNumberOfDealsToTrack > 0 && latestDealIdFromFilscan > 0) {
      let processedCount = 0;
      for (let i = latestDealIdFromFilscan; i > lastDeal; i--) {
        const response = await axios.post(lotusApi, {
          id: 1,
          jsonrpc: '2.0',
          method: 'Filecoin.StateMarketStorageDeal',
          params: [i, null]
        }, { headers });
        if (response.data.result && response.data.result.Proposal.Client === clientId) {
          this.logger.debug(`Process ${client} ${clientId} ${maxNumberOfDealsToTrack} ${latestDealIdFromFilscan} ${lastDeal}`);
          processedCount++;
          if (processedCount >= maxNumberOfDealsToTrack) {
            this.logger.info(`Reached the maximum number of deals to track according to filscan ${maxNumberOfDealsToTrack}`);
            break;
          }
          const result = response.data.result;
          const pieceCid = result.Proposal.PieceCID['/'];
          const provider = result.Proposal.Provider;
          const dataCid = result.Proposal.Label;
          const pieceSize = result.Proposal.PieceSize;
          const startEpoch = result.Proposal.StartEpoch;
          const endEpoch = result.Proposal.EndEpoch;

          const existingDeal = await Datastore.DealStateModel.findOne({
            dealId: i
          });
          if (existingDeal) { // Deal state will be updated later by updateDealFromLotus
            continue;
          }
          const newlyProposedDeal = await Datastore.DealStateModel.findOne({
            pieceCid,
            provider,
            client,
            state: 'proposed'
          });

          if (newlyProposedDeal) {
            await Datastore.DealStateModel.updateOne({
              _id: newlyProposedDeal._id
            }, {
              $set: {
                dealId: i,
                state: 'published'
              }
            });
            this.logger.debug(`Deal ${i} was proposed through singularity. Filling in deal ID.`);
          } else {
            await Datastore.DealStateModel.create({
              client,
              provider,
              dealId: i,
              // dealCid: deal['cid'],
              dataCid,
              pieceCid,
              pieceSize,
              startEpoch,
              expiration: endEpoch,
              duration: endEpoch - startEpoch,
              state: 'published'
            });
            this.logger.debug(`Deal ${i} inserted as published.`);
          }
        }
      }
    }
  }

  private async markExpiredDeals (client: string): Promise<void> {
    const chainHeight = HeightFromCurrentTime() - 120;
    let modified = (await Datastore.DealStateModel.updateMany({
      client,
      state: {
        $in: ['published', 'proposed']
      },
      startEpoch: {
        $gt: 0,
        $lt: chainHeight
      }
    }, {
      $set: {
        state: 'proposal_expired'
      }
    })).modifiedCount;
    this.logger.info(`Marked ${modified} deals as proposal_expired from ${client}`);

    modified = (await Datastore.DealStateModel.updateMany({
      client,
      state: {
        $in: ['active']
      },
      expiration: {
        $gt: 0,
        $lt: chainHeight
      }
    }, {
      $set: {
        state: 'expired'
      }
    })).modifiedCount;
    this.logger.info(`Marked ${modified} deals as expired from ${client}`);
  }

  private async updateDealFromLotus (client: string): Promise<void> {
    this.logger.debug('Start update deal state from lotus.', { client });
    const api = config.get<string>('deal_tracking_service.lotus_api');
    const token = config.get<string>('deal_tracking_service.lotus_token');
    for await (const dealState of Datastore.DealStateModel.find({
      client,
      state: 'published'
    })) {
      const headers: AxiosRequestHeaders = {};
      if (token !== '') {
        headers['Authorization'] = `Bearer ${token}`;
      }
      this.logger.debug(`Fetching from ${api}`, { dealId: dealState.dealId });
      const response = await axios.post(api, {
        id: 1,
        jsonrpc: '2.0',
        method: 'Filecoin.StateMarketStorageDeal',
        params: [dealState.dealId, null]
      }, { headers });
      if (response.data.error && response.data.error.code === 1) {
        await Datastore.DealStateModel.findByIdAndUpdate(dealState.id, {
          state: 'slashed'
        });
      } else {
        const result = response.data.result;
        const expiration: number = result.Proposal.EndEpoch;
        const slashed = result.State.SlashEpoch > 0;
        const pieceCid = result.Proposal.PieceCID['/'];
        const dealActive = result.State.SectorStartEpoch > 0;
        if (slashed) {
          await Datastore.DealStateModel.findByIdAndUpdate(dealState.id, {
            pieceCid, expiration, state: 'slashed'
          });
          this.logger.warn(`Deal ${dealState.dealId} is slashed.`);
        } else if (dealActive) {
          await Datastore.DealStateModel.findByIdAndUpdate(dealState.id, {
            pieceCid, expiration, state: 'active'
          });
          this.logger.info(`Deal ${dealState.dealId} is active on chain.`);
          if (dealState.dealCid && dealState.dealCid !== '') {
            await MetricEmitter.Instance().emit({
              type: 'deal_active',
              values: {
                pieceCid: dealState.pieceCid,
                pieceSize: dealState.pieceSize,
                dataCid: dealState.dataCid,
                provider: dealState.provider,
                client: dealState.client,
                verified: dealState.verified,
                duration: dealState.duration,
                price: dealState.price
              }
            });
          }
        }
      }
    }
  }
}
