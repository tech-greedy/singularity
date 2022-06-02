import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import { constants } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import xbytes from 'xbytes';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreatePreparationRequest from './CreatePreparationRequest';
import DeletePreparationRequest from './DeletePreparationRequest';
import ErrorCode from './ErrorCode';
import GetPreparationDetailsResponse from './GetPreparationDetailsResponse';
import { GetPreparationsResponse } from './GetPreparationsResponse';
import UpdatePreparationRequest from './UpdatePreparationRequest';
import { ObjectId } from 'mongodb';
import GenerationRequest from '../common/model/GenerationRequest';

export default class DealPreparationService extends BaseService {
  private static AllowedDealSizes: number[] = DealPreparationService.initAllowedDealSizes();
  private app: Express = express();

  public constructor () {
    super(Category.DealPreparationService);
    this.handleCreatePreparationRequest = this.handleCreatePreparationRequest.bind(this);
    this.handleUpdatePreparationRequest = this.handleUpdatePreparationRequest.bind(this);
    this.handleRemovePreparationRequest = this.handleRemovePreparationRequest.bind(this);
    this.handleListPreparationRequests = this.handleListPreparationRequests.bind(this);
    this.handleGetPreparationRequest = this.handleGetPreparationRequest.bind(this);
    this.handleGetGenerationRequest = this.handleGetGenerationRequest.bind(this);
    this.startCleanupHealthCheck = this.startCleanupHealthCheck.bind(this);
    if (!this.enabled) {
      this.logger.warn('Service is not enabled. Exit now...');
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
    this.app.delete('/preparation/:id', this.handleRemovePreparationRequest);
    this.app.post('/preparation/:id/:generation', this.handleUpdatePreparationRequest);
    this.app.get('/preparations', this.handleListPreparationRequests);
    this.app.get('/preparation/:id', this.handleGetPreparationRequest);
    this.app.get('/generation/:dataset/:id', this.handleGetGenerationRequest);
    this.app.get('/generation/:id', this.handleGetGenerationRequest);
  }

  public start (): void {
    const bind = config.get<string>('deal_preparation_service.bind');
    const port = config.get<number>('deal_preparation_service.port');
    this.startCleanupHealthCheck();
    this.app!.listen(port, bind, () => {
      this.logger.info(`Service started listening at http://${bind}:${port}`);
    });
  }

  private async cleanupHealthCheck (): Promise<void> {
    this.logger.debug(`Cleaning up health check table`);
    // Find all active workerId
    const workerIds = (await Datastore.HealthCheckModel.find()).map(worker => worker.workerId);
    let modified = (await Datastore.ScanningRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
    if (modified > 0) {
      this.logger.debug(`Reset ${modified} tasks from Scanning Request table`);
    }
    modified = (await Datastore.GenerationRequestModel.updateMany({ workerId: { $nin: workerIds } }, { workerId: null })).modifiedCount;
    if (modified > 0) {
      this.logger.debug(`Reset ${modified} tasks from Generation Request table`);
    }
  }

  private async startCleanupHealthCheck (): Promise<void> {
    await this.cleanupHealthCheck();
    setTimeout(this.startCleanupHealthCheck, 5000);
  }

  private async handleGetGenerationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    const dataset = request.params['dataset'];
    this.logger.info('Received request to get details of dataset generation.', { id, dataset });
    let found;
    const idIsInt = !isNaN(parseInt(id));
    if (ObjectId.isValid(id)) {
      this.logger.debug('id is valid ObjectId');
      found = await Datastore.GenerationRequestModel.findById(id);
    } else if (ObjectId.isValid(dataset) && idIsInt) {
      this.logger.debug('id is valid integer and dataset is valid ObjectId');
      found = await Datastore.GenerationRequestModel.findOne({ index: id, datasetId: dataset });
    } else if (dataset !== undefined && idIsInt) {
      this.logger.debug('id is valid integer and dataset is undefined');
      found = await Datastore.GenerationRequestModel.findOne({ index: id, datasetName: dataset });
    } else {
      this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
      return;
    }
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
      return;
    }

    const result : GenerationRequest = {
      id: found.id,
      datasetId: found.datasetId,
      datasetName: found.datasetName,
      path: found.path,
      index: found.index,
      fileList: found.fileList.map((f) => ({
        path: f.path,
        size: f.size,
        start: f.start,
        end: f.end
      })),
      generatedFileList: found.generatedFileList.map((f) => ({
        path: f.path,
        size: f.size,
        start: f.start,
        end: f.end,
        dir: f.dir,
        selector: f.selector,
        cid: f.cid
      })),
      workerId: found.workerId,
      status: found.status,
      errorMessage: found.errorMessage,
      dataCid: found.dataCid,
      pieceCid: found.pieceCid,
      pieceSize: found.pieceSize,
      carSize: found.carSize
    };
    response.end(JSON.stringify(result));
  }

  private async handleGetPreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    this.logger.info(`Received request to get details of dataset preparation request.`, { id });
    const found = ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : await Datastore.ScanningRequestModel.findOne({ name: id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    const generations = await Datastore.GenerationRequestModel.find({ datasetId: found.id }, { fileList: 0, generatedFileList: 0 });
    const result: GetPreparationDetailsResponse = {
      id: found.id,
      name: found.name,
      path: found.path,
      minSize: found.minSize,
      maxSize: found.maxSize,
      scanningStatus: found.status,
      errorMessage: found.errorMessage,
      generationTotal: await Datastore.GenerationRequestModel.count({ datasetId: found.id }),
      generationActive: await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: 'active' }),
      generationPaused: await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: 'paused' }),
      generationCompleted: await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: 'completed' }),
      generationError: await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: 'error' }),
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
        pieceSize: generation.pieceSize,
        carSize: generation.carSize
      });
    }
    response.end(JSON.stringify(result));
  }

  private async handleListPreparationRequests (_request: Request, response: Response) {
    this.logger.info('Received request to list all preparation requests.');
    const scanningRequests = await Datastore.ScanningRequestModel.find();
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
        generationTotal: await Datastore.GenerationRequestModel.count({ datasetId: r.id }),
        generationActive: await Datastore.GenerationRequestModel.count({ datasetId: r.id, status: 'active' }),
        generationPaused: await Datastore.GenerationRequestModel.count({ datasetId: r.id, status: 'paused' }),
        generationCompleted: await Datastore.GenerationRequestModel.count({ datasetId: r.id, status: 'completed' }),
        generationError: await Datastore.GenerationRequestModel.count({ datasetId: r.id, status: 'error' })
      });
    }
    response.end(JSON.stringify(result));
  }

  private async handleRemovePreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    const generation = request.params['generation'];
    const { purge } = <DeletePreparationRequest>request.body;
    this.logger.info(`Received request to delete dataset preparation request.`, { id, generation, purge });
    const found = ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : await Datastore.ScanningRequestModel.findOne({ name: id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }

    await found.remove();

    const outPath = path.resolve(process.env.NODE_CONFIG_DIR!, config.get('deal_preparation_worker.out_dir'));
    if (purge) {
      for await (const { dataCid } of Datastore.GenerationRequestModel.find({ databaseId: found.id }, { dataCid: 1 })) {
        const filename = path.join(outPath, dataCid + '.car');
        this.logger.info(`Removing file.`, { filename });
        await fs.rm(filename, { force: true });
      }
    }
    await Datastore.GenerationRequestModel.remove({ databaseId: found.id });

    response.end();
  }

  private async handleUpdatePreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    const generation = request.params['generation'];
    const { action } = <UpdatePreparationRequest>request.body;
    this.logger.info(`Received request to update dataset preparation request.`, { id, generation, action });
    if (!['resume', 'pause', 'retry'].includes(action)) {
      this.sendError(response, ErrorCode.ACTION_INVALID);
      return;
    }
    const found = ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : await Datastore.ScanningRequestModel.findOne({ name: id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    const actionMap = {
      resume: [{ status: 'paused' }, { status: 'active' }],
      pause: [{ status: 'active' }, { status: 'paused' }],
      retry: [{ status: 'error' }, { status: 'active', errorMessage: undefined }]
    };

    const changed = (await Datastore.ScanningRequestModel.findOneAndUpdate({
      id: found.id,
      ...actionMap[action][0]
    }, actionMap[action][1])) != null
      ? 1
      : 0;

    let changedGenerations;
    let changedGeneration;
    if (!generation) {
      changedGenerations = (await Datastore.GenerationRequestModel.updateMany({
        id: found.id,
        ...actionMap[action][0]
      }, actionMap[action][1])).modifiedCount;
    } else {
      const generationIsInt = !isNaN(parseInt(generation));
      if (ObjectId.isValid(generation)) {
        changedGeneration = (await Datastore.GenerationRequestModel.findOneAndUpdate(
          { id: generation, ...actionMap[action][0] },
          actionMap[action][1],
          { projection: { _id: 1 } }))
          ? 1
          : 0;
      } else if (generationIsInt) {
        changedGeneration = (await Datastore.GenerationRequestModel.findOneAndUpdate(
          { index: generation, ...actionMap[action][0] },
          actionMap[action][1],
          { projection: { _id: 1 } }))
          ? 1
          : 0;
      } else {
        this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
        return;
      }
    }

    response.end(JSON.stringify({
      scanningRequestsChanged: changed,
      generationRequestsChanged: changedGenerations ?? changedGeneration
    }));
  }

  private sendError (response: Response, error: ErrorCode) {
    this.logger.warn(`Error code`, { error });
    response.status(400);
    response.end(JSON.stringify({ error }));
  }

  private async handleCreatePreparationRequest (request: Request, response: Response) {
    const {
      name,
      path,
      dealSize
    } = <CreatePreparationRequest>request.body;
    this.logger.info(`Received request to start preparing dataset.`, { name, path, dealSize });
    const dealSizeNumber = xbytes.parseSize(dealSize);
    // Validate dealSize
    if (!DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
      this.sendError(response, ErrorCode.DEAL_SIZE_NOT_ALLOWED);
      return;
    }

    const minSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.minDealSizeRatio'));
    const maxSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.maxDealSizeRatio'));
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
      throw e;
    }
    response.end(JSON.stringify({
      id: scanningRequest.id,
      name,
      minSize,
      maxSize,
      path,
      status: scanningRequest.status
    }));
  }

  private static initAllowedDealSizes (): number[] {
    const result = [];
    for (let i = 8; i <= 36; i++) {
      result.push(2 ** i);
    }

    return result;
  }
}
