import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import ErrorCode from '../model/ErrorCode';
import sendError from './ErrorHandler';
import { GeneratedFileList } from '../../common/model/OutputFileList';
import canonicalize from 'canonicalize';

export function getContentsAndGroupings (generatedFileList: GeneratedFileList) {
  const contents: any = {};
  const groupings: any = {};
  for (const fileInfo of generatedFileList) {
    if (fileInfo.path === '') {
      continue;
    }
    if (fileInfo.dir) {
      groupings[fileInfo.path] = fileInfo.cid;
    } else {
      contents[fileInfo.path] = {
        CID: fileInfo.cid,
        filesize: fileInfo.size
      };
      if (fileInfo.start) {
        contents[fileInfo.path].chunkoffset = fileInfo.start;
        contents[fileInfo.path].chunklength = fileInfo.end! - fileInfo.start;
      }
    }
  }

  return [contents, groupings];
}

export default async function handleGetGenerationManifestRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  const dataset = request.params['dataset'];
  this.logger.info(`Received request to get manifest of dataset preparation request.`, { id, dataset });
  const found = await Datastore.findGenerationRequest(id, dataset);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
    return;
  }
  if (found.status !== 'completed') {
    sendError(this.logger, response, ErrorCode.GENERATION_NOT_COMPLETED);
    return;
  }

  const generatedFileList = (await Datastore.OutputFileListModel.find({
    generationId: found.id
  })).map(r => r.generatedFileList).flat();

  const [contents, groupings] = getContentsAndGroupings(generatedFileList);

  const result = {
    piece_cid: found.pieceCid,
    payload_cid: found.dataCid,
    raw_car_file_size: found.carSize,
    dataset: found.datasetName,
    contents,
    groupings
  };
  response.end(canonicalize(result));
}
