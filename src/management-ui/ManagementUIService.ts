import BaseService from '../common/BaseService';
import { Category } from '../common/Logger';
import config from '../common/Config';
import next from 'next';
import http from 'http';

export default class ManagementUIService extends BaseService {

  public constructor () {
    super(Category.ManagementUIService);
    if (!this.enabled) {
      this.logger.warn('Management UI Service is not enabled. Exit now...');
    }
  }

  public start () {
    const bind = config.get<string>('management_ui_service.bind');
    const port = config.get<number>('management_ui_service.port');

    const dev = process.env.NODE_ENV !== 'production';
    const app = next({ dev });
    const handle = app.getRequestHandler();

    app.prepare().then(() => {
      const server = http.createServer((req, res) => {
        handle(req, res);
      });

      server.listen(port, bind, () => {
        this.logger.info(`Management UI started listening at http://${bind}:${port}/mgmt`);
      });
    });
  }
}
