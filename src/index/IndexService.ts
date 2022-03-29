import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import LookupRequest from './LookupRequest';
import ErrorCode from './ErrorCode';
import Datastore from '../common/Datastore';

export default class IndexService extends BaseService {
  private app: Express = express();

  public constructor () {
    super(Category.IndexService);
    this.handleLookupRequest = this.handleLookupRequest.bind(this);
    if (!this.enabled) {
      this.logger.warn('Index Service is not enabled. Exit now...');
      return;
    }
    this.app.use(Logger.getExpressLogger(Category.IndexService));
    this.app.use(bodyParser.urlencoded({ extended: false }));
    this.app.use(bodyParser.json());
    this.app.post('/lookup', this.handleLookupRequest);
  }

  private async handleLookupRequest (request: Request, response: Response): Promise<void> {
    console.log(request.body);
    const {
      datasetName,
      datasetId,
      path
    } = <LookupRequest>request.body;
    this.logger.info(`Looking up files with datasetName: "${datasetName}", datasetId: "${datasetId}", path: "${path}"`);
    if (datasetName === undefined && datasetId === undefined) {
      response.setHeader('Content-Type', 'application/json');
      this.sendError(response, ErrorCode.DATASET_EMPTY);
      return;
    }

    response.setHeader('Content-Type', 'application/x-ndjson');
    const query: any = {};
    if (datasetId) {
      query.datasetId = datasetId;
    }
    if (datasetName) {
      query.datasetName = datasetName;
    }
    if (path) {
      query.filePath = path;
    }
    const resultStream = Datastore.DatasetFileMappingModel.find(query);
    for await (const entry of resultStream) {
      response.write(JSON.stringify({
        datasetId: entry.datasetId,
        datasetName: entry.datasetName,
        path: entry.filePath,
        selector: entry.selector
      }));
      response.write('\n');
    }
    response.end();
  }

  public start (): void {
    const bind = config.get<string>('index_service.bind');
    const port = config.get<number>('index_service.port');
    this.app!.listen(port, bind, () => {
      this.logger.info(`Index Service started listening at http://${bind}:${port}`);
    });
  }

  private sendError (response: Response, error: ErrorCode) {
    this.logger.warn(`Error code - ${error}`);
    response.status(400);
    response.end(JSON.stringify({ error }));
  }
}
