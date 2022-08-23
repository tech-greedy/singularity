import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import ErrorCode from '../model/ErrorCode';
import Datastore from '../../common/Datastore';
import sendError from './ErrorHandler';
import DeletePreparationRequest from '../model/DeletePreparationRequest';
import path from 'path';
import fs from 'fs/promises';

export default async function handleDeletePreparationRequest (this: DealPreparationService, request: Request, response: Response) {

  const id = request.params['id'];
  const generation = request.params['generation'];
  const { purge } = <DeletePreparationRequest>request.body;
  this.logger.info(`Received request to delete dataset preparation request.`, { id, generation, purge });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  if (purge) {
    let removed = 0;
    for await (const { pieceCid } of Datastore.GenerationRequestModel.find({ datasetId: found.id }, { pieceCid: 1 })) {
      if (pieceCid) {
        const file = path.join(found.outDir, pieceCid + '.car');
        this.logger.debug(`Removing file.`, { file });
        await fs.rm(file, { force: true });
        removed += 1;
      }
    }
    this.logger.info(`Removed ${removed} files.`);
  }

  this.logger.debug(`Deleting dataset preparation request.`, { id: found.id });
  await found.delete();
  this.logger.debug(`Deleted dataset preparation request.`);
  let removed = 0;
  for (const generationRequest of await Datastore.GenerationRequestModel.find({ datasetId: found.id })) {
    this.logger.debug(`Deleting generation request and input/output file list.`, { id: generationRequest.id });
    await Datastore.InputFileListModel.deleteMany({ generationId: generationRequest.id });
    await Datastore.OutputFileListModel.deleteMany({ generationId: generationRequest.id });
    await generationRequest.delete();
    this.logger.debug(`Deleted generation request and input/output file list.`);
    removed += 1;
  }
  this.logger.info(`Removed ${removed} generation requests.`);

  response.end();
}
