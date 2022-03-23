import bodyParser from 'body-parser';
import config from 'config';
import express, { Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import Logger, { Category } from '../common/Logger';
import PrepareRequest from './PrepareRequest';

export default class OrchestratorService extends BaseService {
  public constructor () {
    super(Category.Orchestrator);
    this.handlePrepareRequest = this.handlePrepareRequest.bind(this);
    // TODO database maintenance to update workerId to undefined if it is no longer active
  }

  public start (): void {
    if (!this.enabled) {
      this.logger.warn('Orchestrator is not enabled. Exit now...');
      return;
    }
    const bind = config.get<string>('orchestrator.bind');
    const port = config.get<number>('orchestrator.port');
    const app = express();
    app.use(Logger.getExpressLogger(Category.Orchestrator));
    app.use(bodyParser.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    app.post('/prepare', this.handlePrepareRequest);
    app.listen(port, bind, () => {
      this.logger.info(`Orchestrator started listening at http://${bind}:${port}`);
    });
  }

  private async handlePrepareRequest (request: Request, response: Response) {
    const {
      name,
      path,
      minSize,
      maxSize
    } = <PrepareRequest>request.body;
    this.logger.info(`Received request to prepare dataset "${name}" from "${path}". Min size - ${minSize}, Max size - ${maxSize}.`);
    // TODO validate minSize, maxSize, path
    const scanningRequest = new Datastore.ScanningRequestModel();
    scanningRequest.datasetName = name;
    // TODO allow GiB
    scanningRequest.minSize = Number(minSize);
    scanningRequest.maxSize = Number(maxSize);
    scanningRequest.datasetPath = path;
    scanningRequest.completed = false;
    // TODO Handle name conflict
    await scanningRequest.save();
    response.setHeader('Content-Type', 'application/json');
    response.end(JSON.stringify({ status: 'accepted' }));
  }
}
