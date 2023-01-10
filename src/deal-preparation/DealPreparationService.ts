import bodyParser from 'body-parser';
import express, { Express } from 'express';
import { constants } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import config from '../common/Config';
import handleCreatePreparationRequest from './handler/CreatePreparationRequestHandler';
import handleUpdatePreparationRequest from './handler/UpdatePreparationRequestHandler';
import handleDeletePreparationRequest from './handler/DeletePreparationRequestHandler';
import handleListPreparationRequests from './handler/ListPreparationRequestHandlers';
import handleGetPreparationRequest from './handler/GetPreparationRequestHandler';
import handleGetGenerationRequest from './handler/GetGenerationRequestHandler';
import handleUpdateGenerationRequest from './handler/UpdateGenerationRequestHandler';
import handleGetGenerationManifestRequest from './handler/GetGenerationManifestRequestHandler';
import winston from 'winston';
import handleMonitorRequest from './handler/MonitorRequestHandler';
import { AbortSignal } from '../common/AbortSignal';
import { sleep } from '../common/Util';

export default class DealPreparationService extends BaseService {
  static AllowedDealSizes: number[] = DealPreparationService.initAllowedDealSizes();
  private app: Express = express();

  public constructor () {
    super(Category.DealPreparationService);
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
    this.app.post('/preparation', handleCreatePreparationRequest.bind(this));
    this.app.post('/preparation/:id', handleUpdatePreparationRequest.bind(this));
    this.app.delete('/preparation/:id', handleDeletePreparationRequest.bind(this));
    this.app.post('/generation/:dataset/:id', handleUpdateGenerationRequest.bind(this));
    this.app.post('/generation/:dataset', handleUpdateGenerationRequest.bind(this));
    this.app.get('/preparations', handleListPreparationRequests.bind(this));
    this.app.get('/preparation/:id', handleGetPreparationRequest.bind(this));
    this.app.get('/generation/:dataset/:id', handleGetGenerationRequest.bind(this));
    this.app.get('/generation/:id', handleGetGenerationRequest.bind(this));
    this.app.get('/generation-manifest/:dataset/:id', handleGetGenerationManifestRequest.bind(this));
    this.app.get('/generation-manifest/:id', handleGetGenerationManifestRequest.bind(this));
    this.app.get('/monitor', handleMonitorRequest.bind(this));
  }

  public async start (): Promise<void> {
    const bind = config.get<string>('deal_preparation_service.bind');
    const port = config.get<number>('deal_preparation_service.port');
    await this.initialize(() => Promise.resolve(true));
    this.startCleanupHealthCheck();
    this.app!.listen(port, bind, () => {
      this.logger.info(`Service started listening at http://${bind}:${port}`);
    });
  }

  public static async cleanupIncompleteFiles (logger: winston.Logger) : Promise<void> {
    let dirs = (await Datastore.ScanningRequestModel.find()).map(r => r.outDir);
    dirs = [...new Set(dirs)];
    for (const dir of dirs) {
      try {
        await fs.access(dir, constants.F_OK);
      } catch (e) {
        logger.warn(`${dir} cannot be removed during cleanup.`, e);
        continue;
      }
      for (const file of await fs.readdir(dir)) {
        const regex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.car$/;
        if (regex.test(file)) {
          const fullPath = path.join(dir, file);
          logger.info(`Removing temporary file ${fullPath}`);
          try {
            await fs.rm(fullPath);
          } catch (e) {
            logger.warn(`${fullPath} cannot be removed during cleanup.`, e);
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
          logger.warn(`${dir} cannot be removed during cleanup.`, e);
          continue;
        }
        for (const file of await fs.readdir(dir)) {
          const regex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
          if (regex.test(file)) {
            const fullPath = path.join(dir, file);
            logger.info(`Removing temporary folder ${fullPath}`);
            try {
              await fs.rm(fullPath, { recursive: true, force: true });
            } catch (e) {
              logger.warn(`${fullPath} cannot be removed during cleanup.`, e);
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

  private async startCleanupHealthCheck (abortSignal?: AbortSignal): Promise<void> {
    while (true) {
      if (abortSignal && await abortSignal()) {
        return;
      }
      await sleep(30000);
      await this.cleanupHealthCheck();
    }
  }

  private static initAllowedDealSizes (): number[] {
    const result = [];
    for (let i = 8; i <= 36; i++) {
      result.push(2 ** i);
    }

    return result;
  }
}
