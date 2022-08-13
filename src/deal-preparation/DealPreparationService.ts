import bodyParser from 'body-parser';
import express, { Express, Request, Response } from 'express';
import { constants } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import xbytes from 'xbytes';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import CreatePreparationRequest from './model/CreatePreparationRequest';
import DeletePreparationRequest from './model/DeletePreparationRequest';
import ErrorCode from './model/ErrorCode';
import GetPreparationDetailsResponse from './model/GetPreparationDetailsResponse';
import { GetPreparationsResponse } from './model/GetPreparationsResponse';
import UpdatePreparationRequest from './model/UpdatePreparationRequest';
import { ObjectId } from 'mongodb';
import GenerationRequest from '../common/model/GenerationRequest';
import { GeneratedFileList } from '../common/model/OutputFileList';
import config from '../common/Config';

export default class DealPreparationService extends BaseService {
  static AllowedDealSizes: number[] = DealPreparationService.initAllowedDealSizes();
  private app: Express = express();

  public constructor () {
    super(Category.DealPreparationService);
    this.handleCreatePreparationRequest = this.handleCreatePreparationRequest.bind(this);
    this.handleUpdateGenerationRequest = this.handleUpdateGenerationRequest.bind(this);
    this.handleUpdatePreparationRequest = this.handleUpdatePreparationRequest.bind(this);
    this.handleRemovePreparationRequest = this.handleRemovePreparationRequest.bind(this);
    this.handleListPreparationRequests = this.handleListPreparationRequests.bind(this);
    this.handleGetPreparationRequest = this.handleGetPreparationRequest.bind(this);
    this.handleGetGenerationRequest = this.handleGetGenerationRequest.bind(this);
    this.handleGetGenerationManifestRequest = this.handleGetGenerationManifestRequest.bind(this);
    this.handleMonitorRequest = this.handleMonitorRequest.bind(this);
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
    this.app.post('/generation/:dataset/:id', this.handleUpdateGenerationRequest);
    this.app.post('/generation/:dataset', this.handleUpdateGenerationRequest);
    this.app.get('/preparations', this.handleListPreparationRequests);
    this.app.get('/preparation/:id', this.handleGetPreparationRequest);
    this.app.get('/generation/:dataset/:id', this.handleGetGenerationRequest);
    this.app.get('/generation/:id', this.handleGetGenerationRequest);
    this.app.get('/generation-manifest/:dataset/:id', this.handleGetGenerationManifestRequest);
    this.app.get('/generation-manifest/:id', this.handleGetGenerationManifestRequest);
    this.app.get('/monitor', this.handleMonitorRequest);
  }

  public start (): void {
    const bind = config.get<string>('deal_preparation_service.bind');
    const port = config.get<number>('deal_preparation_service.port');
    this.startCleanupHealthCheck();
    this.app!.listen(port, bind, () => {
      this.logger.info(`Service started listening at http://${bind}:${port}`);
    });
  }

  public static async cleanupIncompleteFiles () : Promise<void> {
    let dirs = (await Datastore.ScanningRequestModel.find()).map(r => r.outDir);
    dirs = [...new Set(dirs)];
    for (const dir of dirs) {
      try {
        await fs.access(dir, constants.F_OK);
      } catch (e) {
        console.warn(`${dir} cannot be removed during cleanup.`, { error: e });
        continue;
      }
      for (const file of await fs.readdir(dir)) {
        const regex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.car$/;
        if (regex.test(file)) {
          const fullPath = path.join(dir, file);
          console.log(`Removing temporary file ${fullPath}`);
          try {
            await fs.rm(fullPath);
          } catch (e) {
            console.warn(`${fullPath} cannot be removed during cleanup.`, { error: e });
          }
        }
      }
    }
    let tmpDirs = (await Datastore.ScanningRequestModel.find()).map(r => r.tmpDir);
    tmpDirs = [...new Set(tmpDirs)];
    for (const dir of tmpDirs) {
      if (dir) {
        try {
          await fs.access(dir, constants.F_OK);
        } catch (e) {
          console.warn(`${dir} cannot be removed during cleanup.`, { error: e });
          continue;
        }
        for (const file of await fs.readdir(dir)) {
          const regex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
          if (regex.test(file)) {
            const fullPath = path.join(dir, file);
            console.info(`Removing temporary folder ${fullPath}`);
            try {
              await fs.rm(fullPath, { recursive: true, force: true });
            } catch (e) {
              console.warn(`${fullPath} cannot be removed during cleanup.`, { error: e });
            }
          }
        }
      }
    }
  }

  private async cleanupHealthCheck (): Promise<void> {
    this.logger.debug(`Cleaning up health check table`);
    // Find all active workerId
    const workerIds = [...(await Datastore.HealthCheckModel.find()).map(worker => worker.workerId), null];
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

  private async handleMonitorRequest (_request: Request, response: Response) {
    const result = (await Datastore.HealthCheckModel.find()).map(h => ({
      downloadSpeed: h.downloadSpeed, workerId: h.workerId
    }));

    response.end(JSON.stringify(result));
  }

  private async getGenerationRequest (request: Request, response: Response): Promise<GenerationRequest | undefined> {
    const id = request.params['id'];
    const dataset = request.params['dataset'];
    this.logger.info('Received request to get details of dataset generation.', { id, dataset });
    let found;
    const idIsInt = !isNaN(parseInt(id));
    if (ObjectId.isValid(id)) {
      this.logger.debug('id is valid ObjectId');
      found = await Datastore.GenerationRequestModel.findById(id);
    } else if (idIsInt) {
      this.logger.debug('id is valid integer and dataset is valid ObjectId');
      found = await Datastore.GenerationRequestModel.findOne({ index: id, datasetName: dataset }) ??
        await Datastore.GenerationRequestModel.findOne({ index: id, datasetId: dataset });
    } else {
      this.sendError(response, ErrorCode.INVALID_OBJECT_ID);
      return undefined;
    }
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
      return undefined;
    }

    return found;
  }

  public static getContentsAndGroupings (generatedFileList: GeneratedFileList) {
    const contents: any = {};
    const groupings: any = {};
    for (const fileInfo of generatedFileList) {
      if (fileInfo.path === '') {
        continue;
      }
      if (fileInfo.dir) {
        groupings[fileInfo.path] = fileInfo.cid;
      } else {
        contents[fileInfo.path] = {
          CID: fileInfo.cid,
          filesize: fileInfo.size
        };
        if (fileInfo.start) {
          contents[fileInfo.path].chunkoffset = fileInfo.start;
          contents[fileInfo.path].chunklength = fileInfo.end! - fileInfo.start;
        }
      }
    }

    return [contents, groupings];
  }

  private async handleGetGenerationManifestRequest (request: Request, response: Response) {
    const found = await this.getGenerationRequest(request, response);
    if (!found) {
      return;
    }
    if (found.status !== 'completed') {
      this.sendError(response, ErrorCode.GENERATION_NOT_COMPLETED);
      return;
    }

    const generatedFileList = (await Datastore.OutputFileListModel.find({
      generationId: found.id
    })).map(r => r.generatedFileList).flat();

    const [contents, groupings] = DealPreparationService.getContentsAndGroupings(generatedFileList);

    const result = {
      piece_cid: found.pieceCid,
      payload_cid: found.dataCid,
      raw_car_file_size: found.carSize,
      dataset: found.datasetName,
      contents,
      groupings
    };
    response.end(JSON.stringify(result));
  }

  private async handleGetGenerationRequest (request: Request, response: Response) {
    const found = await this.getGenerationRequest(request, response);
    if (!found) {
      return;
    }

    const fileList = (await Datastore.InputFileListModel.find({
      generationId: found.id
    })).map(r => r.fileList).flat().map(r => ({
      path: r.path, size: r.size, start: r.start, end: r.end
    }));
    const generatedFileList = (await Datastore.OutputFileListModel.find({
      generationId: found.id
    })).map(r => r.generatedFileList).flat().map(r => ({
      path: r.path, size: r.size, start: r.start, end: r.end, dir: r.dir, cid: r.cid
    }));

    const result = {
      id: found.id,
      datasetId: found.datasetId,
      datasetName: found.datasetName,
      path: found.path,
      index: found.index,
      outDir: found.outDir,
      fileList,
      generatedFileList,
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
    const found = await Datastore.ScanningRequestModel.findOne({ name: id }) ??
      (ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : undefined);
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
      outDir: found.outDir,
      scanningStatus: found.status,
      scanned: found.scanned,
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
        outDir: r.outDir,
        scanningStatus: r.status,
        scanned: r.scanned,
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
    const found = await Datastore.ScanningRequestModel.findOne({ name: id }) ??
      (ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : undefined);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }

    if (purge) {
      for await (const { dataCid, pieceCid } of Datastore.GenerationRequestModel.find({ datasetId: found.id }, { dataCid: 1, pieceCid: 1 })) {
        if (dataCid) {
          const file = path.join(found.outDir, dataCid + '.car');
          this.logger.info(`Removing file.`, { file });
          await fs.rm(file, { force: true });
        }
        if (pieceCid) {
          const file = path.join(found.outDir, pieceCid + '.car');
          this.logger.info(`Removing file.`, { file });
          await fs.rm(file, { force: true });
        }
      }
    }

    await found.delete();
    for (const generationRequest of await Datastore.GenerationRequestModel.find({ datasetId: found.id })) {
      await Datastore.InputFileListModel.deleteMany({ generationId: generationRequest.id });
      await Datastore.OutputFileListModel.deleteMany({ generationId: generationRequest.id });
      await generationRequest.delete();
    }

    response.end();
  }

  private async handleUpdatePreparationRequest (request: Request, response: Response) {
    const id = request.params['id'];
    const { action } = <UpdatePreparationRequest>request.body;
    this.logger.info(`Received request to update dataset preparation request.`, { id, action });
    if (!['resume', 'pause', 'retry'].includes(action)) {
      this.sendError(response, ErrorCode.ACTION_INVALID);
      return;
    }
    const found = await Datastore.ScanningRequestModel.findOne({ name: id }) ??
      (ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : undefined);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    const actionMap = {
      resume: [{ status: 'paused' }, { status: 'active', workerId: null }],
      pause: [{ status: 'active' }, { status: 'paused', workerId: null }],
      retry: [{ status: 'error' }, { status: 'active', $unset: { errorMessage: 1 }, workerId: null }]
    };

    const changed = (await Datastore.ScanningRequestModel.findOneAndUpdate({
      _id: found.id,
      ...actionMap[action][0]
    }, actionMap[action][1])) != null
      ? 1
      : 0;

    response.end(JSON.stringify({
      scanningRequestsChanged: changed
    }));
  }

  private async handleUpdateGenerationRequest (request: Request, response: Response) {
    const dataset = request.params['dataset'];
    const generation = request.params['id'];
    const { action } = <UpdatePreparationRequest>request.body;
    this.logger.info(`Received request to update dataset preparation request.`, { dataset, generation, action });
    if (!['resume', 'pause', 'retry'].includes(action)) {
      this.sendError(response, ErrorCode.ACTION_INVALID);
      return;
    }
    const found = await Datastore.ScanningRequestModel.findOne({ name: dataset }) ??
      (ObjectId.isValid(dataset) ? await Datastore.ScanningRequestModel.findById(dataset) : undefined);
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    const actionMap = {
      resume: [{ status: 'paused' }, { status: 'active', workerId: null }],
      pause: [{ status: 'active' }, { status: 'paused', workerId: null }],
      retry: [{ status: 'error' }, { status: 'active', $unset: { errorMessage: 1 }, workerId: null }]
    };

    let changedGenerations;
    let changedGeneration;
    if (!generation) {
      changedGenerations = (await Datastore.GenerationRequestModel.updateMany({
        datasetId: found.id,
        ...actionMap[action][0]
      }, actionMap[action][1])).modifiedCount;
    } else {
      const generationIsInt = !isNaN(parseInt(generation));
      if (ObjectId.isValid(generation)) {
        changedGeneration = (await Datastore.GenerationRequestModel.findOneAndUpdate(
          { _id: generation, ...actionMap[action][0] },
          actionMap[action][1],
          { projection: { _id: 1 } }))
          ? 1
          : 0;
      } else if (generationIsInt) {
        changedGeneration = (await Datastore.GenerationRequestModel.findOneAndUpdate(
          { datasetId: found.id, index: generation, ...actionMap[action][0] },
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
      dealSize,
      outDir,
      minRatio,
      maxRatio,
      tmpDir
    } = <CreatePreparationRequest>request.body;
    this.logger.info(`Received request to start preparing dataset.`, { name, path, dealSize });
    const dealSizeNumber = xbytes.parseSize(dealSize);
    // Validate dealSize
    if (!DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
      this.sendError(response, ErrorCode.DEAL_SIZE_NOT_ALLOWED);
      return;
    }
    if (minRatio && (minRatio < 0.5 || minRatio > 0.95)) {
      this.sendError(response, ErrorCode.MIN_RATIO_INVALID);
      return;
    }
    if (maxRatio && (maxRatio < 0.5 || maxRatio > 0.95)) {
      this.sendError(response, ErrorCode.MAX_RATIO_INVALID);
      return;
    }
    if (minRatio && maxRatio && minRatio >= maxRatio) {
      this.sendError(response, ErrorCode.MAX_RATIO_INVALID);
      return;
    }

    let minSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.minDealSizeRatio'));
    if (minRatio) {
      minSize = minRatio * dealSizeNumber;
    }
    minSize = Math.round(minSize);
    let maxSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.maxDealSizeRatio'));
    if (maxRatio) {
      maxSize = maxRatio * dealSizeNumber;
    }
    maxSize = Math.round(maxSize);
    if (path.startsWith('s3://') && !tmpDir) {
      this.sendError(response, ErrorCode.TMPDIR_MISSING_FOR_S3);
    }
    try {
      if (!path.startsWith('s3://')) {
        await fs.access(path, constants.F_OK);
      }
      await fs.access(outDir, constants.F_OK);
      if (tmpDir) {
        await fs.access(tmpDir, constants.F_OK);
      }
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
    scanningRequest.outDir = outDir;
    scanningRequest.tmpDir = tmpDir;
    scanningRequest.scanned = 0;
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
      outDir,
      tmpDir,
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
