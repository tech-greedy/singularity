import config from 'config';
import express, { Express } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import path from 'path';

export default class HttpHostingService extends BaseService {
  private app: Express = express();

  public constructor () {
    super(Category.HttpHostingService);
    if (!this.enabled) {
      this.logger.warn('Service is not enabled. Exit now...');
      return;
    }
    this.app.use(Logger.getExpressLogger(Category.HttpHostingService));
    const staticPath = path.resolve(process.env.NODE_CONFIG_DIR!, config.get<string>('http_hosting_service.static_path'));
    this.app.use(express.static(staticPath));
  }

  public start (): void {
    const bind = config.get<string>('http_hosting_service.bind');
    const port = config.get<number>('http_hosting_service.port');
    this.app!.listen(port, bind, () => {
      this.logger.info(`Started listening at http://${bind}:${port}`);
    });
  }
}
