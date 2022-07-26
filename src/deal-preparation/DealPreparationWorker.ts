import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';
import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import { randomUUID } from 'crypto';
import BaseService from '../common/BaseService';
import Datastore from '../common/Datastore';
import { Category } from '../common/Logger';
import GenerationRequest from '../common/model/GenerationRequest';
import ScanningRequest from '../common/model/ScanningRequest';
import Scanner from './Scanner';
import fs from 'fs-extra';
import path from 'path';
import { performance } from 'perf_hooks';
import { GeneratedFileList } from '../common/model/OutputFileList';
import { FileInfo, FileList } from '../common/model/InputFileList';
import GenerateCar from '../common/GenerateCar';
import { pipeline } from 'stream/promises';
import { S3Client, GetObjectCommand, GetObjectCommandInput } from '@aws-sdk/client-s3';
import NoopRequestSigner from './NoopRequestSigner';
import winston from 'winston';

interface IpldNode {
  Name: string,
  Hash: string,
  Size: number,
  Link: IpldNode[] | null
}

interface CidMapType {[key: string] : {
  IsDir: boolean, Cid: string
}}

export interface GenerateCarOutput {
  Ipld: IpldNode,
  PieceSize: number,
  PieceCid: string,
  DataCid: string,
  CidMap: CidMapType
}

export default class DealPreparationWorker extends BaseService {
  private readonly workerId: string;

  public constructor () {
    super(Category.DealPreparationWorker);
    this.workerId = randomUUID();
    this.startHealthCheck = this.startHealthCheck.bind(this);
    this.startPollWork = this.startPollWork.bind(this);
  }

  public start (): void {
    if (!this.enabled) {
      this.logger.warn('Worker is not enabled. Exit now...');
    }

    this.startHealthCheck();
    this.startPollWork();
  }

  private async scan (request: ScanningRequest): Promise<void> {
    this.logger.info(`Started scanning.`, { id: request.id, name: request.name, path: request.path, minSize: request.minSize, maxSize: request.maxSize });
    let index = 0;
    for (const createdGenerationRequest of await Datastore.GenerationRequestModel.find({ datasetId: request.id, status: 'created' })) {
      this.logger.info(`Deleting pending generation requests`, { id: createdGenerationRequest.id });
      await Datastore.InputFileListModel.deleteMany({ generationId: createdGenerationRequest.id });
      await createdGenerationRequest.delete();
    }
    const lastGeneration = await Datastore.GenerationRequestModel.findOne({ datasetId: request.id, status: { $ne: 'created' } }, { _id: 1, index: 1 }, { sort: { index: -1 } });
    let lastFileInfo: FileInfo | undefined;
    if (lastGeneration) {
      const lastFileList = await Datastore.InputFileListModel.findOne({ generationId: lastGeneration.id }, undefined, { sort: { index: -1 } });
      lastFileInfo = lastFileList!.fileList[lastFileList!.fileList.length - 1];
      this.logger.info(`Resuming scanning. Start from ${lastFileInfo!.path}, offset: ${lastFileInfo!.end}.`);
    }
    for await (const fileList of Scanner.scan(request.path, request.minSize, request.maxSize, lastFileInfo)) {
      if (!await Datastore.ScanningRequestModel.findById(request.id)) {
        this.logger.info('The scanning request has been removed. Scanning stopped.', { id: request.id, name: request.name });
        return;
      }
      if ((await Datastore.ScanningRequestModel.findById(request.id))?.status === 'paused') {
        this.logger.info(`Scanning request has been paused.`, { id: request.id, name: request.name });
        return;
      }
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: request.id,
        datasetName: request.name,
        path: request.path,
        outDir: request.outDir,
        tmpDir: request.tmpDir,
        index,
        status: 'created'
      });
      this.logger.info('Created a new generation request.', { id: request.id, name: request.name, index });
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
        this.logger.debug('Created new INPUT file list for the generation request.', { id: request.id, name: request.name, index, from: i, to: i + 1000 });
      }
      await Datastore.GenerationRequestModel.findByIdAndUpdate(generationRequest.id, {
        status: 'active'
      }, { projection: { _id: 1 } });
      this.logger.info('Marking generation request to active', { id: request.id, name: request.name, index });
      index++;
    }
    await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { status: 'completed', workerId: null });
    this.logger.info(`Finished scanning. Marking scanning to completed. Inserted ${index} generation tasks.`);
  }

  public static async moveS3FileList (fileList: FileList, parentPath: string, tmpDir: string, logger?: winston.Logger): Promise<void> {
    const s3Path = parentPath.slice('s3://'.length);
    const bucketName = s3Path.split('/')[0];
    const region = await Scanner.detectS3Region(bucketName);
    const client = new S3Client({ region, signer: new NoopRequestSigner() });
    for (const fileInfo of fileList) {
      const key = fileInfo.path.slice('s3://'.length + bucketName.length + 1);
      const commandInput : GetObjectCommandInput = {
        Bucket: bucketName,
        Key: key
      };
      if (fileInfo.start !== undefined && fileInfo.end !== undefined) {
        commandInput.Range = `bytes=${fileInfo.start}-${fileInfo.end - 1}`;
      }
      const command = new GetObjectCommand(commandInput);
      // For S3 bucket, always use the path that contains the bucketName - TODO override this behavior with a flag
      const rel = fileInfo.path.slice('s3://'.length);
      const dest = path.resolve(tmpDir, rel);
      const destDir = path.dirname(dest);
      await fs.mkdirp(destDir);
      const response = await client.send(command);
      const writeStream = fs.createWriteStream(dest);
      logger?.debug(`Download from ${fileInfo.path} to ${dest}`, { start: fileInfo.start, end: fileInfo.end });
      await pipeline(response.Body, writeStream);
      fileInfo.path = dest;
    }
  }

  public static async moveFileList (fileList: FileList, parentPath: string, tmpDir: string, logger?: winston.Logger): Promise<void> {
    for (const fileInfo of fileList) {
      const rel = path.relative(parentPath, fileInfo.path);
      const dest = path.resolve(tmpDir, rel);
      const destDir = path.dirname(dest);
      await fs.mkdirp(destDir);
      if (fileInfo.start === undefined || fileInfo.end === undefined || (fileInfo.start === 0 && fileInfo.end === fileInfo.size)) {
        logger?.debug(`Copy from ${fileInfo.path} to ${dest}`);
        await fs.copyFile(fileInfo.path, dest);
      } else {
        const readStream = fs.createReadStream(fileInfo.path, {
          start: fileInfo.start,
          end: fileInfo.end - 1
        });
        const writeStream = fs.createWriteStream(dest);
        logger?.debug(`Partial Copy from ${fileInfo.path} to ${dest}`, { start: fileInfo.start, end: fileInfo.end });
        await pipeline(readStream, writeStream);
      }
      fileInfo.path = dest;
    }
  }

  private async generate (request: GenerationRequest, fileList: FileList, tmpDir: string | undefined)
    : Promise<[stdout: string, stderr: string, statusCode: number | null, signalCode: NodeJS.Signals | null]> {
    await fs.mkdir(request.outDir, { recursive: true });
    if (tmpDir) {
      if (request.path.startsWith('s3://')) {
        await DealPreparationWorker.moveS3FileList(fileList, request.path, tmpDir, this.logger);
      } else {
        await DealPreparationWorker.moveFileList(fileList, request.path, tmpDir, this.logger);
      }
      tmpDir = path.resolve(tmpDir);
    }
    this.logger.debug(`Spawning generate-car.`, { outPath: request.outDir, parentPath: request.path, tmpDir });
    const input = JSON.stringify(fileList.map(file => ({
      Path: file.path,
      Size: file.size,
      Start: file.start,
      End: file.end
    })));
    const [stdout, stderr, exitCode, signalCode] = await DealPreparationWorker.invokeGenerateCar(request.id, input, request.outDir, tmpDir ?? request.path);
    this.logger.debug(`Child process finished.`, { stdout, stderr, exitCode, signalCode });
    return [stdout, stderr, exitCode, signalCode];
  }

  private static async checkPauseOrRemove (generationId: string, child: ChildProcessWithoutNullStreams) {
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
    setTimeout(() => DealPreparationWorker.checkPauseOrRemove(generationId, child), 5000);
  }

  public static async invokeGenerateCar (generationId: string | undefined, input: string, outDir: string, p: string)
    : Promise<[stdout: string, stderr: string, statusCode: number | null, signalCode: NodeJS.Signals | null]> {
    const cmd = GenerateCar.path!;
    const args = ['-o', outDir, '-p', p];
    const child = spawn(cmd, args, {
      stdio: ['pipe', 'pipe', 'pipe']
    });
    (async () => {
      await streamWrite(child.stdin, input);
      await streamEnd(child.stdin);
    })();
    if (generationId) {
      DealPreparationWorker.checkPauseOrRemove(generationId, child);
    }
    const stdout = await readableToString(child.stdout);
    let stderr = '';
    child.stderr.on('data', function (chunk) {
      stderr += chunk;
    });
    try {
      await onExit(child);
    } catch (_) {}
    return [stdout, stderr, child.exitCode, child.signalCode];
  }

  private async pollScanningWork (): Promise<boolean> {
    const newScanningWork = await Datastore.ScanningRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId
    });
    if (newScanningWork) {
      this.logger.info(`${this.workerId} - Polled a new scanning request.`, { name: newScanningWork.name, id: newScanningWork.id });
      try {
        await this.scan(newScanningWork);
      } catch (err) {
        if (err instanceof Error) {
          this.logger.error(`${this.workerId} - Encountered an error.`, err);
          await Datastore.ScanningRequestModel.findByIdAndUpdate(newScanningWork.id, { status: 'error', errorMessage: err.message, workerId: null });
          return true;
        }
        throw err;
      }
    }

    return newScanningWork != null;
  }

  private async pollGenerationWork (): Promise<boolean> {
    const newGenerationWork = await Datastore.GenerationRequestModel.findOneAndUpdate({
      workerId: null,
      status: 'active'
    }, {
      workerId: this.workerId,
      generatedFileList: []
    }, {
      new: true
    });
    if (newGenerationWork) {
      this.logger.info(`${this.workerId} - Polled a new generation request.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index });
      const fileList = (await Datastore.InputFileListModel.find({
        generationId: newGenerationWork.id
      }, null, { sort: { index: 1 } })).map(r => r.fileList).flat();
      let timeSpentInMs = performance.now();
      let tmpDir : string | undefined;
      if (newGenerationWork.tmpDir) {
        tmpDir = path.join(newGenerationWork.tmpDir, randomUUID());
      }
      let result;
      try {
        result = await this.generate(newGenerationWork, fileList, tmpDir);
      } finally {
        if (tmpDir) {
          await fs.rm(tmpDir, { recursive: true });
        }
      }
      timeSpentInMs = performance.now() - timeSpentInMs;

      // Parse the output and update the database
      const [stdout, stderr, statusCode, signalCode] = result!;
      if (statusCode !== 0) {
        this.logger.error(`${this.workerId} - Encountered an error.`, { stderr, statusCode, signalCode });
        await Datastore.GenerationRequestModel.findOneAndUpdate({ _id: newGenerationWork.id, status: 'active' }, { status: 'error', errorMessage: stderr, workerId: null }, { projection: { _id: 1 } });
        return true;
      }

      const output :GenerateCarOutput = JSON.parse(stdout);
      const carFile = path.join(newGenerationWork.outDir, output.PieceCid + '.car');
      const carFileStat = await fs.stat(carFile);
      const fileMap = new Map<string, FileInfo>();
      const parentPath = tmpDir ?? newGenerationWork.path;
      for (const fileInfo of fileList) {
        fileMap.set(path.relative(parentPath, fileInfo.path).split(path.sep).join('/'), fileInfo);
      }
      const generatedFileList = DealPreparationWorker.handleGeneratedFileList(fileMap, output.CidMap);
      if (!await Datastore.ScanningRequestModel.findById(newGenerationWork.datasetId)) {
        this.logger.info('Scanning request has been removed. Give up updating the generation request', { datasetId: newGenerationWork.datasetId, datasetName: newGenerationWork.datasetName });
        return true;
      }
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
        this.logger.debug('Created new OUTPUT file list for the generation request.', { id: newGenerationWork.id, name: newGenerationWork.datasetName, index: newGenerationWork.index, from: i, to: i + 1000 });
      }
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
      this.logger.info(`${this.workerId} - Finished Generation of dataset.`,
        { id: newGenerationWork.id, datasetName: newGenerationWork.datasetName, index: newGenerationWork.index, timeSpentInMs: timeSpentInMs });
    }

    return newGenerationWork != null;
  }

  public static handleGeneratedFileList (
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

  private readonly PollInterval = 5000;

  private readonly ImmediatePollInterval = 1;

  private async startPollWork (): Promise<void> {
    let hasDoneWork = false;
    try {
      hasDoneWork = await this.pollWork();
    } catch (error) {
      this.logger.error(this.workerId, error);
    }
    if (hasDoneWork) {
      setTimeout(this.startPollWork, this.ImmediatePollInterval);
    } else {
      setTimeout(this.startPollWork, this.PollInterval);
    }
  }

  private async pollWork (): Promise<boolean> {
    this.logger.debug(`${this.workerId} - Polling for work`);
    let hasDoneWork = await this.pollScanningWork();
    if (!hasDoneWork) {
      hasDoneWork = await this.pollGenerationWork();
    }

    return hasDoneWork;
  }

  private async startHealthCheck (): Promise<void> {
    await this.healthCheck();
    setTimeout(this.startHealthCheck, 5000);
  }

  private async healthCheck (): Promise<void> {
    this.logger.debug(`${this.workerId} - Sending HealthCheck`);
    await Datastore.HealthCheckModel.findOneAndUpdate(
      {
        workerId: this.workerId
      },
      {},
      {
        upsert: true
      }
    );
  }
}
