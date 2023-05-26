import BaseService from '../common/BaseService';
import express, { Express } from 'express';
import { Category } from '../common/Logger';
import config from '../common/Config';
import next from 'next';

export default class ManagementUIService extends BaseService {

  private server: Express = express();

  public constructor () {
    super(Category.ManagementUIService);
    if (!this.enabled) {
      this.logger.warn('Management UI Service is not enabled. Exit now...');
    }
  }

  public async start () {
    const bind = config.get<string>('management_ui_service.bind');
    const port = config.get<number>('management_ui_service.port');

    const dev = process.env.NODE_ENV !== 'production';
    const app = next({ dev });
    const handle = app.getRequestHandler();

    app.prepare().then(() => {
      this.server.get('*', (req, res) => {
        return handle(req, res)
      })
    
      this.server.listen(port, bind, () => {
        this.logger.info(`Management UI started listening at http://${bind}:${port}/mgmt`);
      });
    });
  }
}
