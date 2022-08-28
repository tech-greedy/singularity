import Datastore from '../../common/Datastore';
import { performance } from 'perf_hooks';
import path from 'path';
import { randomUUID } from 'crypto';
import fs from 'fs-extra';
import { FileInfo, FileList } from '../../common/model/InputFileList';
import winston from 'winston';
import GenerationRequest from '../../common/model/GenerationRequest';
import { moveFileList, MoveResult, moveS3FileList } from './MoveProcessor';
import { ChildProcessPromise, ErrorWithOutput, Output, spawn } from 'promisify-child-process';
import GenerateCar from '../../common/GenerateCar';
import config from '../../common/Config';
import { GeneratedFileList } from '../../common/model/OutputFileList';
import TrafficMonitor from './TrafficMonitor';
import DealPreparationWorker from '../DealPreparationWorker';

export let childProcessPid: number | undefined;

type ChildProcessOutput = Output;

interface ProcessGenerationOutput {
  tmpDir?: string;
  finished: boolean;
}

interface CidMapType {[key: string] : {
    IsDir: boolean, Cid: string
  }}

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[] | null
}

export interface GenerateCarOutput {
  Ipld: IpldNode,
  PieceSize: number,
  PieceCid: string,
  DataCid: string,
  CidMap: CidMapType
}

export async function processGeneration (
  worker: DealPreparationWorker,
  newGenerationWork: GenerationRequest)
  : Promise<ProcessGenerationOutput> {
  const logger = worker.logger;
  const trafficMonitor = worker.trafficMonitor;
  const returnValue: ProcessGenerationOutput = {
    finished: false
  };
  // Get all the file lists for this generation request, in sorted order.
  let fileList = (await Datastore.InputFileListModel.find({
    generationId: newGenerationWork.id
  }))
    .sort((a, b) => a.index - b.index)
    .map(r => r.fileList).flat();
  let timeSpentInMs = performance.now();
  if (newGenerationWork.tmpDir) {
    returnValue.tmpDir = path.join(newGenerationWork.tmpDir, randomUUID());
  }
  await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: worker.workerId }, { $set: { state: 'generation_moving_to_tmpdir' } });
  const moveResult = await moveToTmpdir(logger, trafficMonitor, newGenerationWork, fileList, returnValue.tmpDir);
  if (moveResult.aborted) {
    return returnValue;
  }

  if (moveResult.skipped.size > 0) {
    fileList = fileList.filter(f => !moveResult.skipped.has(f));
  }
  await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: worker.workerId }, { $set: { state: 'generation_generating_car_and_commp' } });
  const result = await generate(logger, newGenerationWork, fileList, returnValue.tmpDir);
  timeSpentInMs = performance.now() - timeSpentInMs;

  const {
    stdout,
    stderr,
    code
  } = result;

  await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: worker.workerId }, { $set: { state: 'generation_parsing_output' } });
  // Handle the error and update the database
  if (code !== 0) {
    logger.error(`${worker.workerId} Encountered an error.`, result);
    await Datastore.GenerationRequestModel.findOneAndUpdate({
      _id: newGenerationWork.id,
      status: 'active'
    }, {
      status: 'error',
      errorMessage: stderr,
      workerId: null
    }, { projection: { _id: 1 } });
    return returnValue;
  }

  // Parse the output
  const output: GenerateCarOutput = JSON.parse(stdout?.toString() ?? '');
  const carFile = path.join(newGenerationWork.outDir, output.PieceCid + '.car');
  const carFileStat = await fs.stat(carFile);
  const fileMap = new Map<string, FileInfo>();
  const parentPath = returnValue.tmpDir ?? newGenerationWork.path;

  // Populate the map from relative path to fileInfo
  for (const fileInfo of fileList) {
    fileMap.set(path.relative(parentPath, fileInfo.path).split(path.sep).join('/'), fileInfo);
  }

  // Create the output file list
  const generatedFileList = handleGeneratedFileList(fileMap, output.CidMap);

  // Check if the scanning request is still there
  if (!await Datastore.ScanningRequestModel.findById(newGenerationWork.datasetId)) {
    logger.info(`${worker.workerId} Scanning request has been removed. Give up updating the generation request`, {
      datasetId: newGenerationWork.datasetId,
      datasetName: newGenerationWork.datasetName
    });
    return returnValue;
  }

  await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: worker.workerId }, { $set: { state: 'generation_saving_output' } });
  // Populate the output file list with the generated file list
  for (let i = 0; i < generatedFileList.length; i += 1000) {
    await Datastore.OutputFileListModel.updateOne({
      generationId: newGenerationWork.id,
      index: i / 1000
    },
    {
      $setOnInsert: {
        generatedFileList: generatedFileList.slice(i, i + 1000)
      }
    },
    {
      upsert: true,
      projection: { _id: 1 }
    });
    logger.debug(`${worker.workerId} Created new OUTPUT file list for the generation request.`, {
      id: newGenerationWork.id,
      name: newGenerationWork.datasetName,
      index: newGenerationWork.index,
      from: i,
      to: i + 1000
    });
  }

  // Update the generation request with metadata
  await Datastore.GenerationRequestModel.findByIdAndUpdate(newGenerationWork.id, {
    status: 'completed',
    dataCid: output.DataCid,
    pieceSize: output.PieceSize,
    pieceCid: output.PieceCid,
    carSize: carFileStat.size,
    $unset: { errorMessage: 1 },
    workerId: null
  }, {
    projection: { _id: 1 }
  });

  logger.info(`${worker.workerId} Finished Generation of dataset.`,
    {
      id: newGenerationWork.id,
      datasetName: newGenerationWork.datasetName,
      index: newGenerationWork.index,
      timeSpentInMs: timeSpentInMs
    });
  returnValue.finished = true;
  return returnValue;
}

async function moveToTmpdir (
  logger: winston.Logger,
  trafficMonitor: TrafficMonitor,
  request: GenerationRequest,
  fileList: FileList,
  tmpDir: string | undefined): Promise<MoveResult> {
  await fs.mkdir(request.outDir, { recursive: true });
  let moveResult : MoveResult = {
    aborted: false, skipped: new Set<FileInfo>()
  };
  if (tmpDir) {
    if (request.path.startsWith('s3://')) {
      try {
        moveResult = await moveS3FileList(
          logger,
          fileList,
          request.path,
          tmpDir,
          request.skipInaccessibleFiles,
          trafficMonitor.countNewChunk.bind(trafficMonitor),
          () => isGenerationRequestNoLongerActive(request.id));
      } finally {
        trafficMonitor.downloaded = 0;
      }
    } else {
      moveResult = await moveFileList(
        logger,
        fileList,
        request.path,
        tmpDir,
        request.skipInaccessibleFiles,
        () => isGenerationRequestNoLongerActive(request.id));
    }
  }
  return moveResult;
}

export async function generate (logger: winston.Logger, request: GenerationRequest, fileList: FileList, tmpDir: string | undefined)
  : Promise<ChildProcessOutput> {
  logger.debug(`Spawning generate-car.`, {
    outPath: request.outDir,
    parentPath: request.path,
    tmpDir
  });
  let input: string;
  if (tmpDir) {
    tmpDir = path.resolve(tmpDir);
    input = JSON.stringify(fileList.map(file => ({
      Path: file.path,
      Size: file.end !== undefined ? file.end - file.start! : file.size
    })));
  } else {
    input = JSON.stringify(fileList.map(file => ({
      Path: file.path,
      Size: file.size,
      Start: file.start,
      End: file.end
    })));
  }
  const output = await invokeGenerateCar(request.id, input, request.outDir, tmpDir ?? request.path);
  logger.debug(`Child process finished.`, output);
  return output;
}

async function isGenerationRequestNoLongerActive (id: string) : Promise<boolean> {
  return (await Datastore.GenerationRequestModel.findById(id))?.status !== 'active';
}

export async function invokeGenerateCar (generationId: string | undefined, input: string, outDir: string, p: string)
  : Promise<ChildProcessOutput> {
  const cmd = GenerateCar.path!;
  const args = ['-o', outDir, '-p', p];
  const child = spawn(cmd, args, {
    encoding: 'utf8',
    maxBuffer: config.getOrDefault('deal_preparation_worker.max_buffer', 1024 * 1024 * 1024)
  });
  if (generationId) {
    checkPauseOrRemove(generationId, child);
  }
  child.stdin!.write(input);
  child.stdin!.end();
  childProcessPid = child.pid;
  try {
    return await child;
  } catch (error: any) {
    return <ErrorWithOutput>error;
  } finally {
    childProcessPid = undefined;
  }
}

async function checkPauseOrRemove (generationId: string, child: ChildProcessPromise) {
  const generation = await Datastore.GenerationRequestModel.findById(generationId);
  if (generation?.status !== 'active') {
    try {
      child.kill();
    } catch (_) {
    }
    return;
  }
  if (child.exitCode) {
    return;
  }
  setTimeout(() => checkPauseOrRemove(generationId, child), 5000);
}

export function handleGeneratedFileList (
  fileMap: Map<string, FileInfo>,
  cidMap: CidMapType) : GeneratedFileList {
  const list: GeneratedFileList = [];
  for (const path in cidMap) {
    if (cidMap[path].IsDir) {
      list.push({
        path,
        dir: cidMap[path].IsDir,
        cid: cidMap[path].Cid
      });
      continue;
    }
    const fileInfo = fileMap.get(path)!;
    list.push({
      path,
      dir: cidMap[path].IsDir,
      cid: cidMap[path].Cid,
      size: fileInfo.size,
      start: fileInfo.start,
      end: fileInfo.end
    });
  }
  return list;
}
