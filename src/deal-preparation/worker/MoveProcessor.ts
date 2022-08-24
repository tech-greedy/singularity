import { FileInfo, FileList } from '../../common/model/InputFileList';
import winston from 'winston';
import Scanner from '../scanner/Scanner';
import { GetObjectCommand, GetObjectCommandInput, S3Client } from '@aws-sdk/client-s3';
import NoopRequestSigner from '../../common/s3/NoopRequestSigner';
import { getRetryStrategy } from '../../common/s3/S3RetryStrategy';
import config from '../../common/Config';
import path from 'path';
import fs from 'fs-extra';
import stream, { TransformCallback } from 'stream';
import { pipeline } from 'stream/promises';
import pAll from 'p-all';
import { AbortSignal } from '../../common/AbortSignal';

export interface MoveResult {
  aborted: boolean,
  skipped: Set<FileInfo>
}

export async function moveS3FileList (
  logger: winston.Logger,
  fileList: FileList,
  parentPath: string,
  tmpDir: string,
  skipInaccessibleFiles?: boolean,
  chunkReceivedCallback?: (chunk: any) => void,
  abortSignal?: AbortSignal)
  : Promise<MoveResult> {
  const s3Path = parentPath.slice('s3://'.length);
  const bucketName = s3Path.split('/')[0];
  const region = await Scanner.detectS3Region(bucketName);
  const client = new S3Client({
    region,
    signer: new NoopRequestSigner(),
    retryStrategy: getRetryStrategy()
  });
  const concurrency: number = config.getOrDefault('s3.per_job_concurrency', 4);
  let aborted = false;
  const skipped = new Set<FileInfo>();
  const jobs = function * generator () {
    for (const fileInfo of fileList) {
      yield async (): Promise<void> => {
        if (aborted || (abortSignal && await abortSignal())) {
          aborted = true;
          return;
        }
        try {
          const key = fileInfo.path.slice('s3://'.length + bucketName.length + 1);
          const commandInput: GetObjectCommandInput = {
            Bucket: bucketName,
            Key: key
          };
          if (fileInfo.start !== undefined && fileInfo.end !== undefined) {
            commandInput.Range = `bytes=${fileInfo.start}-${fileInfo.end - 1}`;
          }
          const command = new GetObjectCommand(commandInput);
          // For S3 bucket, always use the path that contains the bucketName
          const rel = fileInfo.path.slice('s3://'.length);
          const dest = path.resolve(tmpDir, rel);
          const destDir = path.dirname(dest);
          await fs.mkdirp(destDir);
          logger.debug(`Download from ${fileInfo.path} to ${dest}`, {
            start: fileInfo.start,
            end: fileInfo.end
          });
          const response = await client.send(command);
          const writeStream = fs.createWriteStream(dest);
          const transform = new stream.Transform({
            transform (chunk: any, encoding: BufferEncoding, callback: TransformCallback) {
              if (chunkReceivedCallback) {
                chunkReceivedCallback(chunk);
              }
              this.push(chunk, encoding);
              callback();
            }
          });
          await pipeline(response.Body, transform, writeStream);
          fileInfo.path = dest;
        } catch (error: any) {
          logger.warn(`Encountered an error when downloading ${fileInfo.path} - ${error}`);
          if (error.Code !== 'AccessDenied' || !skipInaccessibleFiles) {
            throw error;
          }
          skipped.add(fileInfo);
        }
      };
    }
  };
  await pAll(jobs(), {
    stopOnError: true,
    concurrency
  });
  return {
    aborted, skipped
  };
}

export async function moveFileList (logger: winston.Logger, fileList: FileList, parentPath: string, tmpDir: string,
  skipInaccessibleFiles?: boolean, abortSignal?: AbortSignal)
  : Promise<MoveResult> {
  const skipped = new Set<FileInfo>();
  for (const fileInfo of fileList) {
    if (abortSignal && await abortSignal()) {
      return {
        aborted: true,
        skipped
      };
    }
    try {
      const rel = path.relative(parentPath, fileInfo.path);
      const dest = path.resolve(tmpDir, rel);
      const destDir = path.dirname(dest);
      await fs.mkdirp(destDir);
      if (fileInfo.start === undefined || fileInfo.end === undefined || (fileInfo.start === 0 && fileInfo.end === fileInfo.size)) {
        logger.debug(`Copy from ${fileInfo.path} to ${dest}`);
        await fs.copyFile(fileInfo.path, dest);
      } else {
        const readStream = fs.createReadStream(fileInfo.path, {
          start: fileInfo.start,
          end: fileInfo.end - 1
        });
        const writeStream = fs.createWriteStream(dest);
        logger.debug(`Partial Copy from ${fileInfo.path} to ${dest}`, {
          start: fileInfo.start,
          end: fileInfo.end
        });
        await pipeline(readStream, writeStream);
      }
      fileInfo.path = dest;
    } catch (error: any) {
      logger.warn(`Encountered an error when copying ${fileInfo.path} - ${error}`);
      if (error.errno !== -13 || !skipInaccessibleFiles) {
        throw error;
      }
      skipped.add(fileInfo);
    }
  }
  return {
    aborted: false, skipped
  };
}
