import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import ErrorCode from '../model/ErrorCode';
import GetPreparationDetailsResponse from '../model/GetPreparationDetailsResponse';
import sendError from './ErrorHandler';

export default async function handleGetPreparationRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  this.logger.info(`Received request to get details of dataset preparation request.`, { id });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  const generationStats = await Datastore.GenerationRequestModel.aggregate([
    {
      $match: {
        datasetId: found.id
      }
    },
    {
      $group: {
        _id: {
          status: '$status'
        },
        count: { $count: {} }
      }
    }
  ]);

  const active = generationStats.find(s => s._id.status === 'active')?.count ?? 0;
  const paused = generationStats.find(s => s._id.status === 'paused')?.count ?? 0;
  const completed = generationStats.find(s => s._id.status === 'completed')?.count ?? 0;
  const error = generationStats.find(s => s._id.status === 'error')?.count ?? 0;
  const total = generationStats.reduce((acc, s) => acc + s.count, 0);
  const result: GetPreparationDetailsResponse = {
    id: found.id,
    name: found.name,
    path: found.path,
    minSize: found.minSize,
    maxSize: found.maxSize,
    outDir: found.outDir,
    scanningStatus: found.status,
    scanned: found.scanned,
    errorMessage: found.errorMessage,
    generationTotal: total,
    generationActive: active,
    generationPaused: paused,
    generationCompleted: completed,
    generationError: error,
    generationRequests: []
  };

  const generations = await Datastore.GenerationRequestModel.find({ datasetId: found.id }, { fileList: 0, generatedFileList: 0 });
  for (const generation of generations) {
    result.generationRequests.push({
      id: generation.id,
      index: generation.index,
      status: generation.status,
      errorMessage: generation.errorMessage,
      dataCid: generation.dataCid,
      pieceCid: generation.pieceCid,
      pieceSize: generation.pieceSize,
      carSize: generation.carSize
    });
  }
  response.end(JSON.stringify(result));
}
