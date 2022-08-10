import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreateReplicationRequest from './CreateReplicationRequest';
import ErrorCode from './ErrorCode';
import GetReplicationDetailsResponse from './GetReplicationDetailsResponse';
import { GetReplicationsResponse, GetReplicationsResponseItem } from './GetReplicationsResponse';
import UpdateReplicationRequest from './UpdateReplicationRequest';
import { ObjectId } from 'mongodb';
import ObjectsToCsv from 'objects-to-csv';
import GenerateCSVRequest from './GenerateCSVRequest';
import path from 'path';

export default class DealReplicationService extends BaseService {

    private app: Express = express();

    public constructor () {
      super(Category.DealReplicationService);
      this.handleCreateReplicationRequest = this.handleCreateReplicationRequest.bind(this);
      this.handleUpdateReplicationRequest = this.handleUpdateReplicationRequest.bind(this);
      this.handleListReplicationRequests = this.handleListReplicationRequests.bind(this);
      this.handleGetReplicationRequest = this.handleGetReplicationRequest.bind(this);
      this.handlePrintCSVReplicationRequest = this.handlePrintCSVReplicationRequest.bind(this);
      this.startCleanupHealthCheck = this.startCleanupHealthCheck.bind(this);
      if (!this.enabled) {
        this.logger.warn('Deal Replication Service is not enabled. Exit now...');
        return;
      }
      this.app.use(Logger.getExpressLogger(Category.DealReplicationService));
      this.app.use(bodyParser.urlencoded({ extended: false }));
      this.app.use(bodyParser.json());
      this.app.use(function (_req, res, next) {
        res.setHeader('Content-Type', 'application/json');
        next();
      });
      this.app.post('/replication', this.handleCreateReplicationRequest);
      this.app.post('/replication/:id', this.handleUpdateReplicationRequest);
      this.app.get('/replications', this.handleListReplicationRequests);
      this.app.get('/replication/:id', this.handleGetReplicationRequest);
      this.app.post('/replication/:id/csv', this.handlePrintCSVReplicationRequest);
    }

    public start (): void {
      this.startCleanupHealthCheck();
      const bind = config.get<string>('deal_replication_service.bind');
      const port = config.get<number>('deal_replication_service.port');
        this.app!.listen(port, bind, () => {
          this.logger.info(`Deal Replication Service started listening at http://${bind}:${port}`);
        });
    }

    private async handleGetReplicationRequest (request: Request, response: Response) {
      const id = request.params['id'];
      if (!ObjectId.isValid(id)) {
        this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
        return;
      }
      this.logger.info(`Received request to get details of dataset replication request "${id}".`);
      const found = await Datastore.ReplicationRequestModel.findById(id);
      if (!found) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }
      const result: GetReplicationDetailsResponse = {
        id: found.id,
        datasetId: found.datasetId,
        replica: found.maxReplicas,
        storageProviders: found.storageProviders,
        client: found.client,
        urlPrefix: found.urlPrefix,
        maxPrice: found.maxPrice,
        maxNumberOfDeals: found.maxNumberOfDeals,
        isVerfied: String(found.isVerfied),
        startDelay: found.startDelay,
        duration: found.duration,
        isOffline: String(found.isOffline),
        status: found.status,
        cronSchedule: found.cronSchedule,
        cronMaxDeals: found.cronMaxDeals,
        cronMaxPendingDeals: found.cronMaxPendingDeals
      };
      const count = request.query['count'];
      if (count === 'true') {
        result.dealsTotal = await Datastore.DealStateModel.count({ datasetId: found.id });
        result.dealsProposed = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'proposed' });
        result.dealsPublished = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'published' });
        result.dealsActive = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'active' });
        result.dealsProposalExpired = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'proposal_expired' });
        result.dealsExpired = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'expired' });
        result.dealsSlashed = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'slashed' });
        result.dealsError = await Datastore.DealStateModel.count({ datasetId: found.id, status: 'error' });
      }
      response.end(JSON.stringify(result));
    }

    private async handleListReplicationRequests (request: Request, response: Response) {
      const count = request.query['count'];
      this.logger.info('Received request to list all replication requests.');
      const replicationRequests = await Datastore.ReplicationRequestModel.find();
      const result: GetReplicationsResponse = [];
      for (const r of replicationRequests) {
        const obj: GetReplicationsResponseItem = {
          id: r.id,
          datasetId: r.datasetId,
          replica: r.maxReplicas,
          storageProviders: r.storageProviders,
          client: r.client,
          maxNumberOfDeals: r.maxNumberOfDeals,
          status: r.status,
          errorMessage: r.errorMessage
        };
        if (count === 'true') {
          obj.dealsTotal = await Datastore.DealStateModel.count({ datasetId: r.id });
          obj.dealsProposed = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'proposed' });
          obj.dealsPublished = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'published' });
          obj.dealsActive = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'active' });
          obj.dealsProposalExpired = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'proposal_expired' });
          obj.dealsExpired = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'expired' });
          obj.dealsSlashed = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'slashed' });
          obj.dealsError = await Datastore.DealStateModel.count({ datasetId: r.id, status: 'error' });
        }
        result.push(obj);
      }
      response.end(JSON.stringify(result));
    }

    private async handleUpdateReplicationRequest (request: Request, response: Response) {
      const id = request.params['id'];
      if (!ObjectId.isValid(id)) {
        this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
        return;
      }
      const { status, cronSchedule, cronMaxDeals, cronMaxPendingDeals } = <UpdateReplicationRequest>request.body;
      this.logger.info(`Received request to update replication request "${id}" with ` +
      `status: "${status}", cron schedule: ${cronSchedule} and cronMaxDeal: ${cronMaxDeals}.`);
      const found = await Datastore.ReplicationRequestModel.findById(id);
      if (!found) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }
      if (!['active', 'paused'].includes(found.status)) {
        this.sendError(response, ErrorCode.CHANGE_STATE_INVALID);
        return;
      }
      let responseObj;
      if (cronSchedule) {
        if (!found.cronSchedule) {
          this.sendError(response, ErrorCode.NOT_CRON_JOB);
          return;
        }
        responseObj = await Datastore.ReplicationRequestModel.findOneAndUpdate({
          _id: id,
          status: {
            $nin: [
              'completed', 'error'
            ]
          }
        }, {
          cronSchedule,
          cronMaxDeals,
          cronMaxPendingDeals
        }, {
          new: true
        });
      } else if (status) {
        if (!['active', 'paused'].includes(status)) {
          this.sendError(response, ErrorCode.CHANGE_STATE_INVALID);
          return;
        }

        responseObj = await Datastore.ReplicationRequestModel.findOneAndUpdate({
          _id: id,
          status: {
            $nin: [
              'completed', 'error'
            ]
          }
        }, {
          status,
          workerId: null
        }, {
          new: true
        });
      }
      response.end(JSON.stringify(responseObj));
    }

    private sendError (response: Response, error: ErrorCode) {
      this.logger.warn(`Error code - ${error}`);
      response.status(400);
      response.end(JSON.stringify({ error }));
    }

    private async handleCreateReplicationRequest (request: Request, response: Response) {
      const {
        datasetId,
        replica,
        storageProviders,
        client,
        urlPrefix,
        maxPrice,
        isVerfied,
        startDelay,
        duration,
        isOffline,
        maxNumberOfDeals,
        cronSchedule,
        cronMaxDeals,
        cronMaxPendingDeals
      } = <CreateReplicationRequest>request.body;
      this.logger.info(`Received request to replicate dataset "${datasetId}" from client "${client}.`);
      let realDatasetId = datasetId;
      const existingSR = await Datastore.ScanningRequestModel.findOne({
        name: datasetId
      });
      if (existingSR) {
        realDatasetId = existingSR._id.toString();
      }

      // Search GenerationRequest by datasetId, if not even one can be found, immediately return error
      const existingGR = await Datastore.GenerationRequestModel.findOne({
        datasetId: realDatasetId
      });
      if (existingGR == null) {
        this.logger.error(`Did not find any existing GenerationRequest with datasetId ${realDatasetId}`);
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }

      const replicationRequest = new Datastore.ReplicationRequestModel();
      replicationRequest.datasetId = realDatasetId;
      replicationRequest.maxReplicas = replica;
      replicationRequest.storageProviders = storageProviders;
      replicationRequest.client = client;
      replicationRequest.urlPrefix = urlPrefix;
      replicationRequest.maxPrice = maxPrice;
      replicationRequest.isVerfied = isVerfied === 'true';
      replicationRequest.startDelay = startDelay;
      replicationRequest.duration = duration;
      replicationRequest.isOffline = isOffline === 'true';
      replicationRequest.maxNumberOfDeals = maxNumberOfDeals;
      replicationRequest.status = 'active';
      replicationRequest.cronSchedule = cronSchedule;
      replicationRequest.cronMaxDeals = cronMaxDeals;
      replicationRequest.cronMaxPendingDeals = cronMaxPendingDeals;
      try {
        await replicationRequest.save();
        // Create a deal tracking request if not exist
        await Datastore.DealTrackingStateModel.updateOne({
          stateType: 'client',
          stateKey: client
        }, {
          $setOnInsert: {
            stateType: 'client',
            stateKey: client,
            stateValue: 'track'
          }
        }, {
          upsert: true
        });
      } catch (e: any) {
        this.logger.error(`MongoSave error`, e);
        this.sendError(response, ErrorCode.INTERNAL_ERROR);
      }

      response.end(JSON.stringify({ id: replicationRequest.id }));
    }

    private async handlePrintCSVReplicationRequest (request: Request, response: Response) {
      const id = request.params['id'];
      const { outDir } = <GenerateCSVRequest>request.body;
      const replicationRequest = await Datastore.ReplicationRequestModel.findById(id);
      if (replicationRequest) {
        const deals = await Datastore.DealStateModel.find({
          replicationRequestId: id,
          state: { $nin: ['slashed', 'error', 'expired', 'proposal_expired'] }
        });
        this.logger.info(`Found ${deals.length} deals from replication request ${id}`);
        let urlPrefix = replicationRequest.urlPrefix;
        if (!urlPrefix.endsWith('/')) {
          urlPrefix += '/';
        }

        if (deals.length > 0) {
          const csvRow = [];
          for (let i = 0; i < deals.length; i++) {
            const deal = deals[i];
            csvRow.push({
              miner_id: deal.provider,
              deal_cid: deal.dealCid,
              filename: `${deal.pieceCid}.car`,
              piece_cid: deal.pieceCid,
              start_epoch: deal.startEpoch,
              full_url: `${urlPrefix}${deal.pieceCid}.car`
            });
          }
          const csv = new ObjectsToCsv(csvRow);
          const filename = `${outDir}${path.sep}${deals[0].provider}_${id}.csv`;
          await csv.toDisk(filename);
          response.end(`CSV saved to ${filename}`);
        }
      } else {
        this.logger.error(`Could not find replication request ${id}`);
      }
      response.end();
    }

    private async cleanupHealthCheck (): Promise<void> {
      this.logger.debug(`Cleaning up health check table`);
      // Find all active workerId
      const workerIds = [...(await Datastore.HealthCheckModel.find()).map(worker => worker.workerId), null];
      const modified = (await Datastore.ReplicationRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
      if (modified > 0) {
        this.logger.debug(`Reset ${modified} tasks from ReplicationRequestModel table`);
      }
    }

    private async startCleanupHealthCheck (): Promise<void> {
      await this.cleanupHealthCheck();
      setTimeout(this.startCleanupHealthCheck, 5000);
    }
}
