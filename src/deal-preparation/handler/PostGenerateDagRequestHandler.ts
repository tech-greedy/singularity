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

interface GenerateIpldCarOutput {
  DataCid :string
  PieceCid: string
  PieceSize: number
}

export default async function handlePostGenerateDagRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  this.logger.info(`Received request to generate the unixfs dag car.`, { id });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  const checkpoint = performance.now();
  this.logger.debug(`Spawning generate-car.`, {
    outPath: found.outDir,
    dataset: found.name
  });
  const p = found.path.startsWith('s3://') ? found.path.slice(4) : found.path;
  const cmd = GenerateCar.generateIpldCarPath();
  const args = ['-o', found.outDir, '-p', p];
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
        const filePath = fileInfo.path.startsWith('s3://') ? fileInfo.path.slice(4) : fileInfo.path;
        const row = {
          Path: filePath,
          Size: fileInfo.size,
          Start: fileInfo.start || 0,
          End: fileInfo.end || fileInfo.size,
          Cid: fileInfo.cid
        };
        const rowString = JSON.stringify(row) + '\n';
        // Wait for drain if the buffer is full
        if (!child.stdin!.write(rowString)) {
          await new Promise(resolve => child.stdin!.once('drain', resolve));
        }
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
    this.logger.error(`Failed to generate the unixfs dag car.`, {
      code, stdout, stderr
    });
    response.status(500);
    response.end(JSON.stringify({ error: 'Failed to generate the unixfs dag car', code, stderr, stdout }));
    return;
  }

  const timeSpentInGenerationMs = performance.now() - checkpoint;
  const result: GenerateIpldCarOutput = JSON.parse(stdout?.toString() ?? '');
  this.logger.info(`Generated the unixfs dag car.`, {
    timeSpentInGenerationMs,
    dataCid: result.DataCid,
    pieceCid: result.PieceCid,
    pieceSize: result.PieceSize
  });
  const carFile = path.join(found.outDir, result.PieceCid + '.car');
  const carFileStat = await fs.stat(carFile);
  const generationRequest = {
    datasetId: found.id,
    datasetName: found.name,
    path: found.path,
    outDir: found.outDir,
    status: 'dag',
    dataCid: result.DataCid,
    carSize: carFileStat.size,
    pieceCid: result.PieceCid,
    pieceSize: result.PieceSize
  };
  await Datastore.GenerationRequestModel.create(generationRequest);
  response.end(JSON.stringify(generationRequest));
}
