import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import CreatePreparationRequest from '../model/CreatePreparationRequest';
import xbytes from 'xbytes';
import ErrorCode from '../model/ErrorCode';
import config from '../../common/Config';
import fs from 'fs/promises';
import { constants } from 'fs';
import Datastore from '../../common/Datastore';
import sendError from './ErrorHandler';

export interface ValidationResult {
  errorCode?: ErrorCode,
  minSize?: number,
  maxSize?: number
}

export async function validateCreatePreparationRequest (
  path: string,
  dealSize: string,
  outDir: string,
  minRatio: number | undefined,
  maxRatio: number | undefined,
  tmpDir: string | undefined) : Promise<ValidationResult> {
  const dealSizeNumber = xbytes.parseSize(dealSize);
  // Validate dealSize
  if (!DealPreparationService.AllowedDealSizes.includes(dealSizeNumber)) {
    return {
      errorCode: ErrorCode.DEAL_SIZE_NOT_ALLOWED
    };
  }
  if (minRatio && (minRatio < 0.5 || minRatio > 0.95)) {
    return {
      errorCode: ErrorCode.MIN_RATIO_INVALID
    };
  }
  if (maxRatio && (maxRatio < 0.5 || maxRatio > 0.95)) {
    return {
      errorCode: ErrorCode.MAX_RATIO_INVALID
    };
  }
  if (minRatio && maxRatio && minRatio >= maxRatio) {
    return {
      errorCode: ErrorCode.MAX_RATIO_INVALID
    };
  }

  let minSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.minDealSizeRatio'));
  if (minRatio) {
    minSize = minRatio * dealSizeNumber;
  }
  minSize = Math.round(minSize);
  let maxSize = Math.floor(dealSizeNumber * config.get<number>('deal_preparation_service.maxDealSizeRatio'));
  if (maxRatio) {
    maxSize = maxRatio * dealSizeNumber;
  }
  maxSize = Math.round(maxSize);
  if (path.startsWith('s3://') && !tmpDir) {
    return {
      errorCode: ErrorCode.TMPDIR_MISSING_FOR_S3
    };
  }
  try {
    if (!path.startsWith('s3://')) {
      await fs.access(path, constants.F_OK);
    }
    await fs.access(outDir, constants.F_OK);
    if (tmpDir) {
      await fs.access(tmpDir, constants.F_OK);
    }
  } catch (_) {
    return {
      errorCode: ErrorCode.PATH_NOT_ACCESSIBLE
    };
  }

  return {
    minSize, maxSize
  };
}

export default async function handleCreatePreparationRequest (this: DealPreparationService, request: Request, response: Response) {
  const requestBody = <CreatePreparationRequest>request.body;
  const {
    name,
    path,
    outDir,
    tmpDir
  } = requestBody;
  this.logger.info(`Received request to start preparing dataset.`, request.body);
  const { errorCode, minSize, maxSize } = await validateCreatePreparationRequest(
    path, requestBody.dealSize, outDir,
    requestBody.minRatio, requestBody.maxRatio, tmpDir);
  if (errorCode) {
    sendError(this.logger, response, errorCode);
    return;
  }
  const scanningRequest = new Datastore.ScanningRequestModel();
  scanningRequest.name = name;
  scanningRequest.minSize = minSize!;
  scanningRequest.maxSize = maxSize!;
  scanningRequest.path = path;
  scanningRequest.status = 'active';
  scanningRequest.outDir = outDir;
  scanningRequest.tmpDir = tmpDir;
  scanningRequest.scanned = 0;
  try {
    this.logger.info(`Creating dataset preparation request.`, { scanningRequest });
    await scanningRequest.save();
    this.logger.info('Dataset preparation request created.', { id: scanningRequest.id });
  } catch (e: any) {
    if (e.name === 'MongoServerError' && e.code === 11000) {
      sendError(this.logger, response, ErrorCode.DATASET_NAME_CONFLICT);
      return;
    }
    throw e;
  }
  response.end(JSON.stringify({
    id: scanningRequest.id,
    name,
    minSize,
    maxSize,
    path,
    outDir,
    tmpDir,
    status: scanningRequest.status
  }));
}
