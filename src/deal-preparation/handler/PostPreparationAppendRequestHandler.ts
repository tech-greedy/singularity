import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import sendError from './ErrorHandler';
import ErrorCode from '../model/ErrorCode';
import MetricEmitter from "../../common/metrics/MetricEmitter";

export default async function handlePostPreparationAppendRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  this.logger.info(`Received request to append new path to existing dataset.`, { id });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  if (found.status === 'active') {
    sendError(this.logger, response, ErrorCode.SCANNING_ACTIVE);
    return;
  }

  const path = request.body.path;
  await Datastore.ScanningRequestModel.findByIdAndUpdate(found.id, {
    path,
    status: 'active',
    rescanInitiated: true,
    dagGenerationAttempted: false
  })

  await MetricEmitter.Instance().emit({
    type: 'data_preparation_appended',
    values: {
      datasetId: found.id,
      datasetName: found.name,
      minSize: found.minSize,
      maxSize: found.maxSize,
      useS3: path.startsWith('s3://'),
      useTmp: found.tmpDir !== undefined,
      skipInaccessibleFiles: found.skipInaccessibleFiles
    }
  });

  response.end(JSON.stringify({
    id: found.id,
    name: found.name,
    minSize: found.minSize,
    maxSize: found.maxSize,
    path,
    outDir: found.outDir,
    tmpDir: found.tmpDir,
    status: 'active'
  }));
}
