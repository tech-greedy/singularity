import bodyParser from 'body-parser';
import config from 'config';
import express, { Request, Response } from 'express';
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

export default class DealPreparationService extends BaseService {
  private static AllowedDealSizes: number[] = DealPreparationService.initAllowedDealSizes();

  public constructor () {
    super(Category.DealPreparationService);
    this.handleCreatePreparationRequest = this.handleCreatePreparationRequest.bind(this);
    this.handleUpdatePrepararationRequest = this.handleUpdatePrepararationRequest.bind(this);
    this.handleListPreparationRequests = this.handleListPreparationRequests.bind(this);
    this.handleGetPreparationRequest = this.handleGetPreparationRequest.bind(this);
    this.handleGetGenerationRequest = this.handleGetGenerationRequest.bind(this);
  }

  public start (): void {
    if (!this.enabled) {
      this.logger.warn('Orchestrator is not enabled. Exit now...');
      return;
    }
    const bind = config.get<string>('orchestrator.bind');
    const port = config.get<number>('orchestrator.port');
    const app = express();
    app.use(Logger.getExpressLogger(Category.DealPreparationService));
    app.use(bodyParser.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    app.use(function (_req, res, next) {
      res.setHeader('Content-Type', 'application/json');
      next();
    });
    app.post('/preparation', this.handleCreatePreparationRequest);
    app.post('/preparation/:id', this.handleUpdatePrepararationRequest);
    app.get('/preparations', this.handleListPreparationRequests);
    app.get('/preparation/:id', this.handleGetPreparationRequest);
    app.get('/generation/:id', this.handleGetGenerationRequest);
    app.listen(port, bind, () => {
      this.logger.info(`Orchestrator started listening at http://${bind}:${port}`);
    });
  }

  private async handleGetGenerationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    this.logger.info(`Received request to get details of dataset generation request "${id}".`);
    const found = await Datastore.GenerationRequestModel.findOne({ id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
      return;
    }

    response.end(JSON.stringify(found));
  }

  private async handleGetPreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    this.logger.info(`Received request to get details of dataset preparation request "${id}".`);
    const found = await Datastore.ScanningRequestModel.findOne({ id });
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
    const aggregate = async (match: any) => Datastore.GenerationRequestModel.aggregate([{
      $match: match
    }, {
      $group: {
        _id: '$datasetId',
        count: { }
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
        generationTotal: <number> total.find(i => i._id === r.id).count,
        generationActive: <number> active.find(i => i._id === r.id).count,
        generationPaused: <number> paused.find(i => i._id === r.id).count,
        generationCompleted: <number> completed.find(i => i._id === r.id).count,
        generationError: <number> error.find(i => i._id === r.id).count
      });
    }
    response.end(JSON.stringify(result));
  }

  private async handleUpdatePrepararationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    const { status } = <UpdatePreparationRequest>request.body;
    this.logger.info(`Received request to change status of dataset "${id}" to "${status}".`);
    const found = await Datastore.ScanningRequestModel.findOne({ id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }

    // Only allow making change to the request if the scanning has completed
    if (found.status !== 'completed') {
      this.sendError(response, ErrorCode.CANNOT_CHANGE_STATE_IF_SCANNING_NOT_COMPLETE);
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
    if (DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
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
      if (e.code === 11000) {
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
