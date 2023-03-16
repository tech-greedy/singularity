import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import sendError from './ErrorHandler';
import ErrorCode from '../model/ErrorCode';
import { performance } from 'perf_hooks';
import GenerateCar from '../../common/GenerateCar';
import { ErrorWithOutput, Output, spawn } from 'promisify-child-process';
import config from '../../common/Config';
import path from 'path';
import fs from 'fs-extra';
import { getNextPowerOfTwo } from '../../common/Util';
import ScanningRequest from '../../common/model/ScanningRequest';
import winston from 'winston';
import GenerationRequest from '../../common/model/GenerationRequest';

interface GenerateIpldCarOutput {
  DataCid :string
  PieceCid: string
  PieceSize: number
}

export async function generateDag (logger: winston.Logger, found: ScanningRequest) :Promise<GenerationRequest> {
  const checkpoint = performance.now();
  const cmd = GenerateCar.generateIpldCarPath();
  const args = ['-o', found.outDir];
  if (config.getOrDefault('deal_preparation_service.force_max_deal_size', true)) {
    args.push('-s', getNextPowerOfTwo(found.maxSize).toString());
  }
  logger.info(`Spawning generate-ipld-car.`, {
    outPath: found.outDir,
    dataset: found.name,
    args: args,
    cmd: cmd
  });
  const child = spawn(cmd, args, {
    encoding: 'utf8',
    maxBuffer: config.getOrDefault('deal_preparation_worker.max_buffer', 1024 * 1024 * 1024)
  });
  // Start streaming all the files to generate-ipld-car
  for (const generationRequest of await Datastore.GenerationRequestModel.find(
    { datasetId: found.id, status: 'completed' },
    null,
    { sort: { index: 1 } })) {
    const generationId = generationRequest.id;
    for (const outputFileList of await Datastore.OutputFileListModel.find(
      { generationId },
      null,
      { sort: { index: 1 } })) {
      for (const fileInfo of outputFileList.generatedFileList) {
        if (fileInfo.dir) {
          continue;
        }
        const row = {
          Path: fileInfo.path,
          Size: fileInfo.size,
          Start: fileInfo.start || 0,
          End: fileInfo.end || fileInfo.size,
          Cid: fileInfo.cid
        };
        const rowString = JSON.stringify(row) + '\n';
        child.stdin!.write(rowString);
      }
    }
  }
  child.stdin!.end();

  let output : Output;
  try {
    output = await child;
  } catch (error: any) {
    output = <ErrorWithOutput>error;
  }

  const {
    stdout,
    stderr,
    code
  } = output;
  if (code !== 0) {
    logger.error(`Failed to generate the unixfs dag car.`, {
      code, stdout, stderr
    });
    throw new Error(`Failed to generate the unixfs dag car. code: ${code}, stdout: ${stdout}, stderr: ${stderr}`);
  }

  const timeSpentInGenerationMs = performance.now() - checkpoint;
  const result: GenerateIpldCarOutput = JSON.parse(stdout?.toString() ?? '');
  logger.info(`Generated the unixfs dag car.`, {
    timeSpentInGenerationMs,
    dataCid: result.DataCid,
    pieceCid: result.PieceCid,
    pieceSize: result.PieceSize
  });
  const carFile = path.join(found.outDir, result.PieceCid + '.car');
  const carFileStat = await fs.stat(carFile);
  const generationRequest = await Datastore.GenerationRequestModel.findOneAndUpdate(
    { datasetId: found.id, status: 'dag', dataCid: result.DataCid },
    {
      $setOnInsert: {
        datasetId: found.id,
        datasetName: found.name,
        path: found.path,
        outDir: found.outDir,
        status: 'dag',
        dataCid: result.DataCid,
        carSize: carFileStat.size,
        pieceCid: result.PieceCid,
        pieceSize: result.PieceSize
      }
    },
    { upsert: true, new: true }
  );
  return generationRequest;
}

export default async function handlePostGenerateDagRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  this.logger.info(`Received request to generate the unixfs dag car.`, { id });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  try {
    const generationRequest = await generateDag(this.logger, found);
    response.json({
      id: generationRequest.id,
      datasetId: generationRequest.datasetId,
      datasetName: generationRequest.datasetName,
      path: generationRequest.path,
      outDir: generationRequest.outDir,
      status: generationRequest.status,
      dataCid: generationRequest.dataCid,
      carSize: generationRequest.carSize,
      pieceCid: generationRequest.pieceCid,
      pieceSize: generationRequest.pieceSize
    });
  } catch (error: any) {
    this.logger.error(`Failed to generate the unixfs dag car.`, { error: error.message });
    response.status(500);
    response.json({ error: error.message });
  }
}
