import BaseService from '../../common/BaseService';
import express, { Express, Request, Response } from 'express';
import Logger, { Category } from '../../common/Logger';
import bodyParser from 'body-parser';
import CreatePolicyRequest from './model/CreatePolicyRequest';
import Datastore from '../../common/Datastore';
import ErrorCode, { ErrorMessage } from './model/ErrorCode';
import DealSelfServicePolicy from '../../common/model/DealSelfServicePolicy';
import config from '../../common/Config';
import DealReplicationWorker from '../DealReplicationWorker';
import { HeightFromCurrentTime } from '../../common/ChainHeight';
import MetricEmitter from '../../common/metrics/MetricEmitter';
import axios, { AxiosRequestHeaders } from 'axios';

export default class DealReplicationService extends BaseService {
    private app: Express = express();

    private datacapCache = new Map<string, [number, number]>();

    public constructor () {
      super(Category.DealSelfService);
      this.handleCreatePolicy = this.handleCreatePolicy.bind(this);
      this.handleListPolicy = this.handleListPolicy.bind(this);
      this.handleRemovePolicy = this.handleRemovePolicy.bind(this);
      this.handleProposeDeal = this.handleProposeDeal.bind(this);
      this.handleGetPieceCids = this.handleGetPieceCids.bind(this);
      if (!this.enabled) {
        this.logger.warn('Deal Self Service is not enabled. Exit now...');
      }
      this.app.use(Logger.getExpressLogger(Category.DealSelfService));
      this.app.use(bodyParser.urlencoded({ extended: false }));
      this.app.use(bodyParser.json());
      this.app.use(function (_req, res, next) {
        res.setHeader('Content-Type', 'application/json');
        next();
      });
      this.app.post('/policy', this.handleCreatePolicy);
      this.app.get('/policy', this.handleListPolicy);
      this.app.delete('/policy/:id', this.handleRemovePolicy);
      this.app.get('/propose', this.handleProposeDeal);
      this.app.get('/pieceCids', this.handleGetPieceCids);
    }

    public async start (): Promise<void> {
      const bind = config.get<string>('deal_self_service.bind');
      const port = config.get<number>('deal_self_service.port');
      await this.initialize(() => Promise.resolve(true));
      this.app!.listen(port, bind, () => {
        this.logger.info(`Service started listening at http://${bind}:${port}`);
      });
    }

    private sendError (response: Response, error: ErrorCode) {
      this.logger.warn(`Error code`, { error });
      response.status(400);
      response.end(JSON.stringify({ error, message: ErrorMessage[error] }));
    }

    private async handleCreatePolicy (request: Request, response: Response) {
      const {
        client,
        provider,
        dataset,
        minStartDays,
        maxStartDays,
        verified,
        price,
        minDurationDays,
        maxDurationDays
      } = <CreatePolicyRequest>request.body;
      if (client == null || provider == null ||
        minStartDays == null || maxStartDays == null ||
        verified == null || price == null ||
        minDurationDays == null || maxDurationDays == null) {
        this.sendError(response, ErrorCode.INVALID_REQUEST);
        return;
      }
      let datasetName: string | undefined;
      if (dataset !== undefined) {
        const scanning = await Datastore.findScanningRequest(dataset);
        if (!scanning) {
          this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
          return;
        }
        datasetName = scanning.name;
      }

      if (minStartDays < 2 || minStartDays > 30) {
        this.sendError(response, ErrorCode.INVALID_MIN_START_DAYS);
        return;
      }

      if (maxStartDays < 2 || maxStartDays > 30) {
        this.sendError(response, ErrorCode.INVALID_MAX_START_DAYS);
        return;
      }

      if (minStartDays > maxStartDays) {
        this.sendError(response, ErrorCode.INVALID_MIN_MAX_START_DAYS);
        return;
      }

      if (price < 0) {
        this.sendError(response, ErrorCode.INVALID_PRICE);
        return;
      }

      if (minDurationDays < 180 || minDurationDays > 540) {
        this.sendError(response, ErrorCode.INVALID_MIN_DURATION_DAYS);
        return;
      }

      if (maxDurationDays < 180 || maxDurationDays > 540) {
        this.sendError(response, ErrorCode.INVALID_MAX_DURATION_DAYS);
        return;
      }

      if (minDurationDays > maxDurationDays) {
        this.sendError(response, ErrorCode.INVALID_MIN_MAX_DURATION_DAYS);
        return;
      }

      if (maxDurationDays + maxStartDays >= 540) {
        this.sendError(response, ErrorCode.INVALID_MAX_DAYS);
      }

      await Datastore.DealSelfServicePolicyModel.create({
        client,
        provider,
        datasetName,
        minStartDays,
        maxStartDays,
        verified,
        price,
        minDurationDays,
        maxDurationDays
      });

      response.end();
    }

    private async handleListPolicy (_: Request, response: Response) {
      const policies = await Datastore.DealSelfServicePolicyModel.find();
      const result : DealSelfServicePolicy[] = [];
      for (const policy of policies) {
        result.push({
          id: policy.id,
          client: policy.client,
          provider: policy.provider,
          datasetName: policy.datasetName,
          minStartDays: policy.minStartDays,
          maxStartDays: policy.maxStartDays,
          verified: policy.verified,
          price: policy.price,
          minDurationDays: policy.minDurationDays,
          maxDurationDays: policy.maxDurationDays
        });
      }

      response.end(JSON.stringify(result));
    }

    private async handleRemovePolicy (request: Request, response: Response) {
      const id = request.params.id;
      await Datastore.DealSelfServicePolicyModel.deleteOne({ id });
      response.end();
    }

    private async handleProposeDeal (request: Request, response: Response) {
      const client = <string | undefined> request.query.client;
      const provider = <string | undefined> request.query.provider;
      const dataset = <string | undefined> request.query.dataset;
      const startDays = <string | undefined> request.query.startDays;
      const durationDays = <string | undefined> request.query.durationDays;
      const pieceCid = <string | undefined> request.query.pieceCid;
      if (provider === undefined) {
        this.sendError(response, ErrorCode.INVALID_PROVIDER);
        return;
      }
      if (dataset === undefined) {
        this.sendError(response, ErrorCode.INVALID_DATASET);
        return;
      }
      const scanningRequest = await Datastore.findScanningRequest(dataset);
      if (!scanningRequest) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }
      let policies: DealSelfServicePolicy[] = await Datastore.DealSelfServicePolicyModel.find({
        provider, datasetName: scanningRequest.name
      });

      // Find the policy that matches the request
      policies = policies.filter((policy) => {
        if (client !== undefined && policy.client !== client) {
          return false;
        }
        if (startDays !== undefined && (policy.minStartDays > Number(startDays) || policy.maxStartDays < Number(startDays))) {
          return false;
        }
        if (durationDays !== undefined && (policy.minDurationDays > Number(durationDays) || policy.maxDurationDays < Number(durationDays))) {
          return false;
        }
        return true;
      });

      const currentTime = new Date().getTime();
      // Filter the policy that still have enough datacap
      const predicates = await Promise.all(policies.map(async (policy) => {
        if (!policy.verified) {
          return true;
        }
        if (!this.datacapCache.has(policy.client) || currentTime - this.datacapCache.get(policy.client)![0] > 600_000) {
          const lotusApi = config.get<string>('deal_tracking_service.lotus_api');
          const lotusToken = config.get<string>('deal_tracking_service.lotus_token');
          const headers: AxiosRequestHeaders = {};
          if (lotusToken !== '') {
            headers['Authorization'] = `Bearer ${lotusToken}`;
          }
          try {
            const response = await axios.post(lotusApi, {
              id: 1,
              jsonrpc: '2.0',
              method: 'Filecoin.StateVerifiedClientStatus',
              params: [policy.client, null]
            }, { headers });
            this.logger.info(`Get datacap for ${policy.client}`, response.data);
            if (response.data.result != null) {
              this.datacapCache.set(policy.client, [currentTime, Number(response.data.result)]);
            } else {
              this.datacapCache.set(policy.client, [currentTime, 0]);
            }
          } catch (e) {
            this.logger.warn(`Failed to get datacap for ${policy.client}`, e);
          }
        }
        // Sum up the deal sizes of pending deals
        let pending: any = (await Datastore.DealStateModel.aggregate([
          {
            $match: {
              state: { $in: ['proposed', 'published'] },
              client: policy.client,
              verified: policy.verified
            }
          },
          {
            $group: {
              _id: null,
              total: { $sum: '$pieceSize' }
            }
          }
        ]));
        pending = pending[0]?.total ?? 0;
        this.logger.info(`Datacap for ${policy.client} is ${this.datacapCache.get(policy.client)![1]}, pending ${pending}`);
        return this.datacapCache.has(policy.client) && this.datacapCache.get(policy.client)![1] >= pending + 64 * 1024 * 1024 * 1024;
      }));

      policies = policies.filter((_, index) => predicates[index]);

      if (policies.length === 0) {
        this.sendError(response, ErrorCode.NO_MATCHING_POLICY);
        return;
      }

      // Use a random policy from applicable policies
      const policy = policies[Math.floor(Math.random() * policies.length)];

      // Register the client for tracking
      await Datastore.DealTrackingStateModel.updateOne({
        stateType: 'client',
        stateKey: policy.client
      }, {
        $setOnInsert: {
          stateType: 'client',
          stateKey: policy.client,
          stateValue: 'track'
        }
      }, {
        upsert: true
      });

      let pieceToPropose: {pieceCid: string, dataCid: string, pieceSize: number, carSize: number};

      // Check if there is already a deal in progress
      if (pieceCid !== undefined) {
        const proposed = await Datastore.DealStateModel.find({
          provider,
          pieceCid,
          state: {
            $in: ['proposed', 'published', 'active']
          }
        });
        if (proposed.length > 0) {
          this.sendError(response, ErrorCode.ALREADY_PROPOSED);
          return;
        }
        const foundPiece = await Datastore.GenerationRequestModel.findOne({
          pieceCid,
          datasetId: scanningRequest.id
        });
        if (!foundPiece) {
          this.sendError(response, ErrorCode.PIECE_NOT_FOUND);
          return;
        }
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        pieceToPropose = foundPiece;
      } else {
        // Find a pieceCid that has not been proposed if pieceCid is not supplied
        const pieceCids = await this.getPieceCidsToPropose(provider, scanningRequest.id, 1);
        if (pieceCids.length === 0) {
          this.sendError(response, ErrorCode.NO_PIECE_TO_PROPOSE);
          return;
        }
        pieceToPropose = pieceCids[0];
      }

      const startDaysNumber = startDays === undefined ? policy.maxStartDays : Number(startDays);
      const durationDaysNumber = durationDays === undefined ? policy.minDurationDays : Number(durationDays);
      const proposal = await this.proposeDeal(
        scanningRequest.id,
        policy.client, provider,
        pieceToPropose,
        startDaysNumber,
        durationDaysNumber,
        policy.verified,
        policy.price);
      response.end(JSON.stringify({
        proposalId: proposal.dealCid,
        status: proposal.state,
        errorMessage: proposal.errorMsg,
        pieceCid: pieceToPropose.pieceCid,
        pieceSize: pieceToPropose.pieceSize,
        dataCid: pieceToPropose.dataCid,
        carSize: pieceToPropose.carSize,
        client: policy.client,
        provider
      }));
    }

    private async handleGetPieceCids (request: Request, response: Response) {
      const provider = <string> request.query.provider;
      const dataset = <string> request.query.dataset;
      if (provider == null) {
        this.sendError(response, ErrorCode.INVALID_PROVIDER);
        return;
      }
      if (dataset == null) {
        this.sendError(response, ErrorCode.INVALID_DATASET);
        return;
      }
      const scanning = await Datastore.findScanningRequest(dataset);
      if (!scanning) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }
      const pieceCids = await this.getPieceCidsToPropose(provider, scanning.id, undefined);
      response.end(JSON.stringify(pieceCids.map((pieceCid) => pieceCid.pieceCid)));
    }

    private async getPieceCidsToPropose (
      provider: string,
      datasetId: string,
      limit: number | undefined): Promise<{pieceCid: string, dataCid: string, pieceSize: number, carSize: number}[]> {
      const pipeline = [
        { $match: { datasetId: datasetId, pieceCid: { $nin: [null, ''] } } },
        {
          $lookup: {
            from: 'dealstates',
            localField: 'pieceCid',
            foreignField: 'pieceCid',
            as: 'dealStates'
          }
        },
        {
          $match:
            {
              dealStates:
              {
                $not:
                  {
                    $elemMatch:
                      {
                        provider: provider,
                        state: { $in: ['proposed', 'published', 'active'] }
                      }
                  }
              }
            }
        },
        { $project: { pieceCid: 1, dataCid: 1, pieceSize: 1, carSize: 1 } }
      ];
      if (limit !== undefined) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        pipeline.push({ $limit: limit });
      }
      return Datastore.GenerationRequestModel.aggregate(pipeline);
    }

    /* istanbul ignore next */
    private async proposeDeal (
      datasetId: string,
      client: string,
      provider: string,
      generation: {pieceCid: string, dataCid: string, pieceSize: number, carSize: number},
      startDaysNumber: number,
      durationDaysNumber: number,
      verified: boolean,
      price: number): Promise<{
        dealCid : string,
        errorMsg : string,
        state: string
    }> {
      const replicationWorker = new DealReplicationWorker();
      let useLotus = true;
      try {
        useLotus = await replicationWorker.isUsingLotus(provider);
      } catch (error) {
        this.logger.error(`SP ${provider} unknown output from libp2p. Assume lotus.`, error);
      }

      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      const replicationRequest = {
        isOffline: true,
        maxPrice: price,
        duration: durationDaysNumber * 2880,
        startDelay: startDaysNumber * 2880
      };
      const currentHeight = HeightFromCurrentTime();
      const startEpoch = startDaysNumber * 2880 + currentHeight;

      let dealCmd = '';
      try {
        dealCmd = await replicationWorker.createDealCmd(useLotus, provider, {
          isOffline: true,
          duration: durationDaysNumber * 2880,
          client,
          isVerfied: verified,
          maxPrice: price,
          urlPrefix: ''
        }, generation, startEpoch);
      } catch (error) {
        this.logger.error(`Deal CMD generation failed`, error);
      }

      const {
        dealCid,
        errorMsg,
        state
      } = await replicationWorker.makeDeal(dealCmd, generation.pieceCid, provider, 0, useLotus, 0, 0);

      if (state === 'proposed') {
        await MetricEmitter.Instance().emit({
          type: 'deal_proposed',
          values: {
            protocol: useLotus ? 'lotus' : 'boost',
            pieceCid: generation.pieceCid,
            dataCid: generation.dataCid,
            pieceSize: generation.pieceSize,
            carSize: generation.carSize,
            provider: provider,
            client: client,
            verified: verified,
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
            pieceCid: generation.pieceCid,
            dataCid: generation.dataCid,
            pieceSize: generation.pieceSize,
            carSize: generation.carSize,
            provider: provider,
            client: client,
            verified: verified,
            duration: replicationRequest.duration,
            price: replicationRequest.maxPrice,
            errorMsg: errorMsg
          }
        });
      }

      await Datastore.DealStateModel.create({
        client: client,
        provider: provider,
        dealCid: dealCid,
        dataCid: generation.dataCid,
        pieceCid: generation.pieceCid,
        pieceSize: generation.pieceSize,
        startEpoch: startEpoch,
        expiration: startEpoch + replicationRequest.duration,
        duration: replicationRequest.duration,
        price: replicationRequest.maxPrice, // unit is Fil per epoch per GB
        verified: verified,
        state: state,
        replicationRequestId: 'selfservice',
        datasetId: datasetId,
        errorMessage: errorMsg
      });

      return { dealCid, errorMsg, state };
    }
}
