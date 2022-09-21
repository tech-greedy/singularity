import bodyParser from 'body-parser';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreateReplicationRequest from './model/CreateReplicationRequest';
import ErrorCode, { ErrorMessage } from './model/ErrorCode';
import GetReplicationDetailsResponse from './model/GetReplicationDetailsResponse';
import { GetReplicationsResponse, GetReplicationsResponseItem } from './model/GetReplicationsResponse';
import UpdateReplicationRequest from './model/UpdateReplicationRequest';
import config from '../common/Config';
import { AbortSignal } from '../common/AbortSignal';

export default class DealReplicationService extends BaseService {

    private app: Express = express();

    public constructor () {
      super(Category.DealReplicationService);
      this.handleCreateReplicationRequest = this.handleCreateReplicationRequest.bind(this);
      this.handleUpdateReplicationRequest = this.handleUpdateReplicationRequest.bind(this);
      this.handleListReplicationRequests = this.handleListReplicationRequests.bind(this);
      this.handleGetReplicationRequest = this.handleGetReplicationRequest.bind(this);
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
      this.logger.info(`Received request to get details of dataset replication request "${id}".`);
      const found = await Datastore.findReplicationRequest(id);
      const verbose = request.query['verbose'] === 'true';
      if (!found) {
        this.sendError(response, ErrorCode.REPLICATION_NOT_FOUND);
        return;
      }
      const dealStateStats = await Datastore.DealStateModel.aggregate([
        {
          $match: {
            replicationRequestId: found.id
          }
        },
        {
          $group: {
            _id: {
              state: '$state'
            },
            count: { $sum: 1 }
          }
        }
      ]);
      const proposed = dealStateStats.find(s => s._id.state === 'proposed')?.count ?? 0;
      const published = dealStateStats.find(s => s._id.state === 'published')?.count ?? 0;
      const active = dealStateStats.find(s => s._id.state === 'active')?.count ?? 0;
      const proposalExpired = dealStateStats.find(s => s._id.state === 'proposal_expired')?.count ?? 0;
      const expired = dealStateStats.find(s => s._id.state === 'expired')?.count ?? 0;
      const slashed = dealStateStats.find(s => s._id.state === 'slashed')?.count ?? 0;
      const error = dealStateStats.find(s => s._id.state === 'error')?.count ?? 0;
      const total = dealStateStats.reduce((acc, s) => acc + s.count, 0);
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
        cronMaxPendingDeals: found.cronMaxPendingDeals,
        fileListPath: found.fileListPath,
        notes: found.notes,
        dealsProposed: proposed,
        dealsPublished: published,
        dealsActive: active,
        dealsProposalExpired: proposalExpired,
        dealsExpired: expired,
        dealsSlashed: slashed,
        dealsError: error,
        dealsTotal: total
      };
      if (verbose) {
        result.deals = await Datastore.DealStateModel.find({ replicationRequestId: id });
      }
      response.end(JSON.stringify(result));
    }

    private async handleListReplicationRequests (_request: Request, response: Response) {
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
          cronSchedule: r.cronSchedule,
          cronMaxDeals: r.cronMaxDeals,
          cronMaxPendingDeals: r.cronMaxPendingDeals,
          fileListPath: r.fileListPath,
          notes: r.notes,
          errorMessage: r.errorMessage
        };
        result.push(obj);
      }
      response.end(JSON.stringify(result));
    }

    private async handleUpdateReplicationRequest (request: Request, response: Response) {
      const id = request.params['id'];
      const { status, cronSchedule, cronMaxDeals, cronMaxPendingDeals } = <UpdateReplicationRequest>request.body;
      this.logger.info(`Received request to update replication request "${id}" with ` +
      `status: "${status}", cron schedule: ${cronSchedule} and cronMaxDeal: ${cronMaxDeals}.`);
      const found = await Datastore.findReplicationRequest(id);
      if (!found) {
        this.sendError(response, ErrorCode.REPLICATION_NOT_FOUND);
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
      response.end(JSON.stringify({ error, message: ErrorMessage[error] }));
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
        cronMaxPendingDeals,
        fileListPath,
        notes
      } = <CreateReplicationRequest>request.body;
      this.logger.info(`Received request to replicate dataset "${datasetId}" from client "${client}.`);
      const scanning = await Datastore.findScanningRequest(datasetId);
      if (!scanning) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }
      // Search GenerationRequest by datasetId, if not even one can be found, immediately return error
      const existingGR = await Datastore.GenerationRequestModel.findOne({
        datasetId: scanning.id
      });
      if (existingGR == null) {
        this.logger.error(`Did not find any existing GenerationRequest with datasetId ${scanning.id}`);
        this.sendError(response, ErrorCode.GENERATION_NOT_FOUND);
        return;
      }

      const replicationRequest = new Datastore.ReplicationRequestModel();
      replicationRequest.datasetId = scanning.id;
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
      replicationRequest.fileListPath = fileListPath;
      replicationRequest.notes = notes;
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

    private async cleanupHealthCheck (): Promise<void> {
      this.logger.debug(`Cleaning up health check table`);
      // Find all active workerId
      const workerIds = [...(await Datastore.HealthCheckModel.find()).map(worker => worker.workerId), null];
      const modified = (await Datastore.ReplicationRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
      if (modified > 0) {
        this.logger.debug(`Reset ${modified} tasks from ReplicationRequestModel table`);
      }
    }

    private async startCleanupHealthCheck (abortSignal?: AbortSignal): Promise<void> {
      if (abortSignal && await abortSignal()) {
        return;
      }
      await this.cleanupHealthCheck();
      setTimeout(async () => this.startCleanupHealthCheck(abortSignal), 5000);
    }
}
