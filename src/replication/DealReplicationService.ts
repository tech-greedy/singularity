import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreateReplicationRequest from './CreateReplicationRequest';
import ErrorCode from './ErrorCode';
import GetReplicationDetailsResponse from './GetReplicationDetailsResponse';
import { GetReplicationsResponse } from './GetReplicationsResponse';
import UpdateReplicationRequest from './UpdateReplicationRequest';
import { ObjectId } from 'mongodb';

export default class DealReplicationService extends BaseService {

    private app: Express = express();

    public constructor () {
      super(Category.DealReplicationService);
      this.handleCreateReplicationRequest = this.handleCreateReplicationRequest.bind(this);
      this.handleUpdateReplicationRequest = this.handleUpdateReplicationRequest.bind(this);
      this.handleListReplicationRequests = this.handleListReplicationRequests.bind(this);
      this.handleGetReplicationRequest = this.handleGetReplicationRequest.bind(this);
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
        maxReplicas: found.maxReplicas,
        criteria: found.criteria,
        client: found.client,
        status: found.status
      };
      response.end(JSON.stringify(result));
    }

    private async handleListReplicationRequests (_request: Request, response: Response) {
      this.logger.info('Received request to list all replication requests.');
      const replicationRequests = await Datastore.ReplicationRequestModel.find();
      const result: GetReplicationsResponse = [];
      for (const r of replicationRequests) {
        result.push({
          id: r.id,
          datasetId: r.datasetId,
          maxReplicas: r.maxReplicas,
          criteria: r.criteria,
          client: r.client,
          status: r.status,
          errorMessage: r.errorMessage,
          replicationTotal: await Datastore.ReplicationRequestModel.count({ datasetId: r.id }),
          replicationActive: await Datastore.ReplicationRequestModel.count({ datasetId: r.id, status: 'active' }),
          replicationPaused: await Datastore.ReplicationRequestModel.count({ datasetId: r.id, status: 'paused' }),
          replicationCompleted: await Datastore.ReplicationRequestModel.count({ datasetId: r.id, status: 'completed' }),
          replicationError: await Datastore.ReplicationRequestModel.count({ datasetId: r.id, status: 'error' })
        });
      }
      response.end(JSON.stringify(result));
    }

    private async handleUpdateReplicationRequest (request: Request, response: Response) {
      const id = request.params['id'];
      if (!ObjectId.isValid(id)) {
        this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
        return;
      }
      const { status } = <UpdateReplicationRequest>request.body;
      this.logger.info(`Received request to change status of dataset "${id}" to "${status}".`);
      if (!['active', 'paused'].includes(status)) {
        this.sendError(response, ErrorCode.CHANGE_STATE_INVALID);
        return;
      }
      const found = await Datastore.ReplicationRequestModel.findById(id);
      if (!found) {
        this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
        return;
      }

      await Datastore.ReplicationRequestModel.updateMany({
        status: {
          $nin: [
            'completed', 'error'
          ]
        }
      }, {
        status
      });
      this.logger.info(`Updated status of incomplete replication request to "${status}".`);
      response.end();
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
        criteria,
        client,
        urlPrefix,
        maxPrice,
        isVerfied,
        duration,
        isOffline,
        maxNumberOfDeals
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
      replicationRequest.criteria = criteria;
      replicationRequest.client = client;
      replicationRequest.urlPrefix = urlPrefix;
      replicationRequest.maxPrice = maxPrice;
      replicationRequest.isVerfied = isVerfied === 'true';
      replicationRequest.duration = duration;
      replicationRequest.isOffline = isOffline === 'true';
      replicationRequest.maxNumberOfDeals = maxNumberOfDeals;
      replicationRequest.status = 'active';
      try {
        await replicationRequest.save();
      } catch (e: any) {
        this.logger.error(`MongoSave error`, e);
        this.sendError(response, ErrorCode.INTERNAL_ERROR);
      }

      response.end(JSON.stringify({ id: replicationRequest.id }));
    }
}
