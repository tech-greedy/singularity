import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import { constants } from 'fs';
import fs from 'fs/promises';
import xbytes from 'xbytes';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreatePreparationRequest from './CreatePreparationRequest';
import ErrorCode from './ErrorCode';
import GetPreparationDetailsResponse from './GetPreparationDetailsResponse';
import { GetPreparationsResponse } from './GetPreparationsResponse';
import UpdatePreparationRequest from './UpdatePreparationRequest';
import { ObjectId } from 'mongodb';

export default class DealPreparationService extends BaseService {
  private static AllowedDealSizes: number[] = DealPreparationService.initAllowedDealSizes();
  private app: Express = express();

  public constructor () {
    super(Category.DealPreparationService);
    this.handleCreatePreparationRequest = this.handleCreatePreparationRequest.bind(this);
    this.handleUpdatePreparationRequest = this.handleUpdatePreparationRequest.bind(this);
    this.handleListPreparationRequests = this.handleListPreparationRequests.bind(this);
    this.handleGetPreparationRequest = this.handleGetPreparationRequest.bind(this);
    this.handleGetGenerationRequest = this.handleGetGenerationRequest.bind(this);
    this.startCleanupHealthCheck = this.startCleanupHealthCheck.bind(this);
    if (!this.enabled) {
      this.logger.warn('Deal Preparation Service is not enabled. Exit now...');
      return;
    }
    this.app.use(Logger.getExpressLogger(Category.DealPreparationService));
    this.app.use(bodyParser.urlencoded({ extended: false }));
    this.app.use(bodyParser.json());
    this.app.use(function (_req, res, next) {
      res.setHeader('Content-Type', 'application/json');
      next();
    });
    this.app.post('/preparation', this.handleCreatePreparationRequest);
    this.app.post('/preparation/:id', this.handleUpdatePreparationRequest);
    this.app.get('/preparations', this.handleListPreparationRequests);
    this.app.get('/preparation/:id', this.handleGetPreparationRequest);
    this.app.get('/generation/:id', this.handleGetGenerationRequest);
  }

  public start (): void {
    const bind = config.get<string>('deal_preparation_service.bind');
    const port = config.get<number>('deal_preparation_service.port');
    this.startCleanupHealthCheck();
    this.app!.listen(port, bind, () => {
      this.logger.info(`Deal Preparation Service started listening at http://${bind}:${port}`);
    });
  }

  private async cleanupHealthCheck (): Promise<void> {
    this.logger.info(`Cleaning up health check table`);
    // Find all active workerId
    const workerIds = (await Datastore.HealthCheckModel.find()).map(worker => worker.workerId);
    let modified = (await Datastore.ScanningRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
    if (modified > 0) {
      this.logger.info(`Reset ${modified} tasks from Scanning Request table`);
    }
    modified = (await Datastore.GenerationRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
    if (modified > 0) {
      this.logger.info(`Reset ${modified} tasks from Generation Request table`);
    }
  }

  private async startCleanupHealthCheck (): Promise<void> {
    await this.cleanupHealthCheck();
    setTimeout(this.startCleanupHealthCheck, 5000);
  }

  private async handleGetGenerationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    if (!ObjectId.isValid(id)) {
      this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
      return;
    }
    this.logger.info(`Received request to get details of dataset generation request "${id}".`);
    const found = await Datastore.GenerationRequestModel.findById(id);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
      return;
    }

    response.end(JSON.stringify(found));
  }

  private async handleGetPreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    if (!ObjectId.isValid(id)) {
      this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
      return;
    }
    this.logger.info(`Received request to get details of dataset preparation request "${id}".`);
    const found = await Datastore.ScanningRequestModel.findById(id);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    const generations = await Datastore.GenerationRequestModel.find({ datasetId: id });
    const result: GetPreparationDetailsResponse = {
      id: found.id,
      name: found.name,
      path: found.path,
      minSize: found.minSize,
      maxSize: found.maxSize,
      scanningStatus: found.status,
      errorMessage: found.errorMessage,
      generationRequests: []
    };
    for (const generation of generations) {
      result.generationRequests.push({
        id: generation.id,
        index: generation.index,
        status: generation.status,
        errorMessage: generation.errorMessage,
        dataCid: generation.dataCid,
        pieceCid: generation.pieceCid,
        pieceSize: generation.pieceSize
      });
    }
    response.end(JSON.stringify(result));
  }

  private async handleListPreparationRequests (_request: Request, response: Response) {
    this.logger.info('Received request to list all preparation requests.');
    const scanningRequests = await Datastore.ScanningRequestModel.find();
    const aggregate = async (match: { status?: string }) => Datastore.GenerationRequestModel.aggregate([{
      $match: match
    }, {
      $group: {
        _id: '$datasetId',
        count: { $count: {} }
      }
    }]);
    const total = await aggregate({});
    const active = await aggregate({ status: 'active' });
    const paused = await aggregate({ status: 'paused' });
    const completed = await aggregate({ status: 'completed' });
    const error = await aggregate({ status: 'error' });
    const result: GetPreparationsResponse = [];
    for (const r of scanningRequests) {
      result.push({
        id: r.id,
        name: r.name,
        path: r.path,
        minSize: r.minSize,
        maxSize: r.maxSize,
        scanningStatus: r.status,
        errorMessage: r.errorMessage,
        generationTotal: <number> total[0].count,
        generationActive: <number> active[0].count,
        generationPaused: <number> paused[0].count,
        generationCompleted: <number> completed[0].count,
        generationError: <number> error[0].count
      });
    }
    response.end(JSON.stringify(result));
  }

  private async handleUpdatePreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    if (!ObjectId.isValid(id)) {
      this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
      return;
    }
    const { status } = <UpdatePreparationRequest>request.body;
    this.logger.info(`Received request to change status of dataset "${id}" to "${status}".`);
    if (!['active', 'paused'].includes(status)) {
      this.sendError(response, ErrorCode.CHANGE_STATE_INVALID);
      return;
    }
    const found = await Datastore.ScanningRequestModel.findById(id);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    // Only allow making change to the request if the scanning has completed
    if (found.status !== 'completed') {
      this.sendError(response, ErrorCode.CANNOT_CHANGE_STATE_IF_SCANNING_NOT_COMPLETE);
      return;
    }

    await Datastore.GenerationRequestModel.updateMany({
      $not: {
        $or: [
          { status: 'completed' },
          { status: 'error' }
        ]
      }
    }, {
      status
    });
    this.logger.info(`Updated status of incomplete generation request to "${status}".`);
    response.end();
  }

  private sendError (response: Response, error: ErrorCode) {
    this.logger.warn(`Error code - ${error}`);
    response.status(400);
    response.end(JSON.stringify({ error }));
  }

  private async handleCreatePreparationRequest (request: Request, response: Response) {
    const {
      name,
      path,
      dealSize
    } = <CreatePreparationRequest>request.body;
    this.logger.info(`Received request to prepare dataset "${name}" from "${path}". Target Deal Size - ${dealSize}.`);
    const dealSizeNumber = xbytes.parseSize(dealSize);
    // Validate dealSize
    if (!DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
      this.sendError(response, ErrorCode.DEAL_SIZE_NOT_ALLOWED);
      return;
    }

    const minSize = Math.floor(dealSizeNumber * 0.55);
    const maxSize = Math.floor(dealSizeNumber * 0.95);
    try {
      await fs.access(path, constants.F_OK);
    } catch (_) {
      this.sendError(response, ErrorCode.PATH_NOT_ACCESSIBLE);
      return;
    }

    const scanningRequest = new Datastore.ScanningRequestModel();
    scanningRequest.name = name;
    scanningRequest.minSize = minSize;
    scanningRequest.maxSize = maxSize;
    scanningRequest.path = path;
    scanningRequest.status = 'active';
    try {
      await scanningRequest.save();
    } catch (e: any) {
      if (e.name === 'MongoServerError' && e.code === 11000) {
        this.sendError(response, ErrorCode.DATASET_NAME_CONFLICT);
        return;
      }
    }

    response.end(JSON.stringify({ id: scanningRequest.id }));
  }

  private static initAllowedDealSizes (): number[] {
    const result = [];
    for (let i = 8; i <= 36; i++) {
      result.push(2 ** i);
    }

    return result;
  }
}
