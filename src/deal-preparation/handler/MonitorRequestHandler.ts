import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';

export default async function handleMonitorRequest (this: DealPreparationService, _request: Request, response: Response) {
  this.logger.debug('Received monitor request');
  const result = (await Datastore.HealthCheckModel.find()).map(h => ({
    downloadSpeed: h.downloadSpeed, workerId: h.workerId, updatedAt: h.updatedAt
  }));

  response.end(JSON.stringify(result));
}
