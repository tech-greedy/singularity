import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import { GetPreparationsResponse } from '../model/GetPreparationsResponse';

export default async function handleListPreparationRequests (this: DealPreparationService, _request: Request, response: Response) {
  this.logger.info('Received request to list all preparation requests.');
  const generationStats = await Datastore.GenerationRequestModel.aggregate([
    {
      $group: {
        _id: {
          datasetId: '$datasetId',
          status: '$status'
        },
        count: { $sum: 1 }
      }
    }
  ]);

  const scanningRequests = await Datastore.ScanningRequestModel.find();
  const result: GetPreparationsResponse = [];
  for (const r of scanningRequests) {
    const active = generationStats.find(s => s._id.datasetId === r.id && s._id.status === 'active')?.count ?? 0;
    const paused = generationStats.find(s => s._id.datasetId === r.id && s._id.status === 'paused')?.count ?? 0;
    const completed = generationStats.find(s => s._id.datasetId === r.id && s._id.status === 'completed')?.count ?? 0;
    const error = generationStats.find(s => s._id.datasetId === r.id && s._id.status === 'error')?.count ?? 0;
    const total = generationStats.filter(s => s._id.datasetId === r.id).reduce((acc, s) => acc + s.count, 0);
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
      generationTotal: total,
      generationActive: active,
      generationPaused: paused,
      generationCompleted: completed,
      generationError: error
    });
  }
  response.end(JSON.stringify(result));
}
