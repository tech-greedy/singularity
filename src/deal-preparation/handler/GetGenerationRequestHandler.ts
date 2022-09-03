import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import ErrorCode from '../model/ErrorCode';
import sendError from './ErrorHandler';

export default async function handleGetGenerationRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  const dataset = request.params['dataset'];
  this.logger.info(`Received request to get details of dataset generation request.`, { id, dataset });
  const found = await Datastore.findGenerationRequest(id, dataset);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
    return;
  }

  const fileList = (await Datastore.InputFileListModel.find({
    generationId: found.id
  })).map(r => r.fileList).flat().map(r => ({
    path: r.path, size: r.size, start: r.start, end: r.end
  }));
  const generatedFileList = (await Datastore.OutputFileListModel.find({
    generationId: found.id
  })).map(r => r.generatedFileList).flat().map(r => ({
    path: r.path, size: r.size, start: r.start, end: r.end, dir: r.dir, cid: r.cid
  }));

  const result = {
    id: found.id,
    datasetId: found.datasetId,
    datasetName: found.datasetName,
    path: found.path,
    index: found.index,
    outDir: found.outDir,
    fileList,
    generatedFileList,
    workerId: found.workerId,
    status: found.status,
    errorMessage: found.errorMessage,
    dataCid: found.dataCid,
    pieceCid: found.pieceCid,
    pieceSize: found.pieceSize,
    carSize: found.carSize
  };
  response.end(JSON.stringify(result));
}
