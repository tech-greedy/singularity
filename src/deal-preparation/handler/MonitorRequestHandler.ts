import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';

export default async function handleMonitorRequest (this: DealPreparationService, _request: Request, response: Response) {
  this.logger.debug('Received monitor request');
  const result = (await Datastore.HealthCheckModel.find()).map(h => ({
    downloadSpeed: h.downloadSpeed,
    workerId: h.workerId,
    updatedAt: h.updatedAt,
    state: h.state,
    pid: h.pid,
    cpuUsage: h.cpuUsage,
    memoryUsage: h.memoryUsage,
    childPid: h.childPid,
    childCpuUsage: h.childCpuUsage,
    childMemoryUsage: h.childMemoryUsage,
    type: h.type
  }));

  response.end(JSON.stringify(result));
}
