import bodyParser from 'body-parser';
import config from 'config';
import express, { Request, Response } from 'express';
import mongoose, { Schema } from 'mongoose';
import winston from 'winston';
import Logger, { Category } from '../common/Logger';

class OrchestratorService {
  private logger: winston.Logger;

  public constructor () {
    this.logger = Logger.GetLogger(Category.Orchestrator);
    this.HandlePrepareRequest = this.HandlePrepareRequest.bind(this);
  }

  public start (): void {
    const enabled = config.get('orchestrator.enabled');
    if (!enabled) {
      this.logger.warn('Orchestrator is not enabled. Exit now...');
      return;
    }
    const bind = config.get<string>('orchestrator.bind');
    const port = config.get<number>('orchestrator.port');
    const app = express();
    app.use(Logger.GetExpressLogger(Category.Orchestrator));
    app.use(bodyParser.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    app.post('/prepare', this.HandlePrepareRequest);
    app.listen(port, bind, () => {
      this.logger.info(`Orchestrator started listening at http://${bind}:${port}`);
    });
  }

  private HandlePrepareRequest (request: Request, response: Response) {
    const {
      name,
      path,
      minSize,
      maxSize
    } = <PrepareRequest>request.body;
    this.logger.info(`Received request to prepare dataset "${name}" from "${path}". Min size - ${minSize}, Max size - ${maxSize}.`);

    const healthCheckSchema = new Schema({
      worker_id: Schema.Types.String,
      last_checked: Schema.Types.Date
    });
    const HealthCheck = mongoose.model('HealthCheck', healthCheckSchema);
    const instance = new HealthCheck();
    instance.s = 'd';
    instance.save();

    response.setHeader('Content-Type', 'application/json');
    response.end(JSON.stringify({}));
  }
}

new OrchestratorService().start();
