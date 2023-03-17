import ScanningRequest from '../../common/model/ScanningRequest';
import Datastore from '../../common/Datastore';
import { FileInfo } from '../../common/model/InputFileList';
import Scanner from '../scanner/Scanner';
import winston from 'winston';
import MetricEmitter from '../../common/metrics/MetricEmitter';
import { performance } from 'perf_hooks';

async function deletePendingGenerationRequests (request: ScanningRequest, logger: winston.Logger) {
  for (const createdGenerationRequest of await Datastore.GenerationRequestModel.find({
    datasetId: request.id,
    status: 'created'
  })) {
    logger.info(`Deleting pending generation requests`, { id: createdGenerationRequest.id });
    await Datastore.InputFileListModel.deleteMany({ generationId: createdGenerationRequest.id });
    await createdGenerationRequest.delete();
  }
}

async function findLastGeneration (request: ScanningRequest, logger: winston.Logger) {
  let index = 0;
  const lastGeneration = await Datastore.GenerationRequestModel.findOne({
    datasetId: request.id,
    status: { $nin: ['created', 'dag'] }
  }, {
    _id: 1,
    index: 1
  }, { sort: { index: -1 } });
  let lastFileInfo: FileInfo | undefined;
  if (lastGeneration) {
    const lastFileList = await Datastore.InputFileListModel.findOne({ generationId: lastGeneration.id }, undefined, { sort: { index: -1 } });
    lastFileInfo = lastFileList!.fileList[lastFileList!.fileList.length - 1];
    logger.info(`Resuming scanning. Start from ${lastFileInfo!.path}, offset: ${lastFileInfo!.end}.`);
    index = lastGeneration.index + 1;
  }
  return {
    index,
    lastFileInfo
  };
}

async function createGenerationRequest (request: ScanningRequest, index: number, logger: winston.Logger, fileList: FileInfo[]) {
  const generationRequest = await Datastore.GenerationRequestModel.create({
    datasetId: request.id,
    datasetName: request.name,
    path: request.path,
    outDir: request.outDir,
    tmpDir: request.tmpDir,
    index,
    status: 'created',
    skipInaccessibleFiles: request.skipInaccessibleFiles
  });
  logger.info('Created a new generation request.', {
    id: request.id,
    name: request.name,
    index
  });
  for (let i = 0; i < fileList.length; i += 1000) {
    await Datastore.InputFileListModel.updateOne({
      generationId: generationRequest.id,
      index: i / 1000
    },
    {
      $setOnInsert: {
        fileList: fileList.slice(i, i + 1000)
      }
    },
    {
      upsert: true,
      projection: { _id: 1 }
    });
    logger.debug('Created new INPUT file list for the generation request.', {
      id: request.id,
      name: request.name,
      index,
      from: i,
      to: i + 1000
    });
  }
  await Datastore.GenerationRequestModel.findByIdAndUpdate(generationRequest.id, {
    status: 'active'
  }, { projection: { _id: 1 } });
  logger.info('Marking generation request to active', {
    id: request.id,
    name: request.name,
    index
  });
  await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { $inc: { scanned: fileList.length } });
}

export default async function scan (logger: winston.Logger, request: ScanningRequest, scanner: Scanner): Promise<void> {
  logger.info(`Started scanning.`, {
    id: request.id,
    name: request.name,
    path: request.path,
    minSize: request.minSize,
    maxSize: request.maxSize
  });

  // Delete all pending generation requests for this scan request.
  await deletePendingGenerationRequests(request, logger);

  // Find last generation request so we can resume
  let {
    index,
    lastFileInfo
  } = await findLastGeneration(request, logger);
  if (request.rescanInitiated === true) {
    lastFileInfo = undefined;
  }
  const checkpoint = performance.now();
  for await (const fileList of scanner.scan(request.path, request.minSize, request.maxSize, lastFileInfo, logger, request.skipInaccessibleFiles)) {
    if (!await Datastore.ScanningRequestModel.findById(request.id)) {
      logger.info('The scanning request has been removed. Scanning stopped.', {
        id: request.id,
        name: request.name
      });
      return;
    }
    await createGenerationRequest(request, index, logger, fileList);
    if (request.rescanInitiated === true) {
      await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, {
        rescanInitiated: false,
      });
      request.rescanInitiated = false;
    }
    index++;
    if ((await Datastore.ScanningRequestModel.findById(request.id))?.status === 'paused') {
      logger.info(`The scanning request has been paused. Scanning stopped.`, {
        id: request.id,
        name: request.name
      });
      return;
    }
  }
  const timeSpendInMs = performance.now() - checkpoint;
  const updated = await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, {
    status: 'completed',
    workerId: null
  }, { new: true });
  await MetricEmitter.Instance().emit({
    type: 'complete_scanning',
    values: {
      datasetId: updated?.id,
      datasetName: updated?.name,
      scanned: updated?.scanned,
      generationTasks: index,
      timeSpendInMs
    }
  });
  logger.info(`Finished scanning. Marking scanning to completed. Inserted ${index} generation tasks.`);
}
