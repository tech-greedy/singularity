import { FileInfo, FileList } from '../../common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { rrdir } from './rrdir';
import { S3Client, ListObjectsV2Command, ListObjectsV2CommandOutput, HeadObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';
import NoopRequestSigner from '../../common/s3/NoopRequestSigner';
import { getRetryStrategy } from '../../common/s3/S3RetryStrategy';
import winston from 'winston';
import fs, { constants } from 'fs-extra';

interface Entry {
  path: string,
  size: number,
  offset?: number,
}

export default class Scanner {
  private s3Client : S3Client | undefined;

  public async initializeS3Client (path: string) :Promise<void> {
    if (path.startsWith('s3://')) {
      const s3Path = path.slice('s3://'.length);
      const bucketName = s3Path.split('/')[0];
      const region = await Scanner.detectS3Region(bucketName);
      this.s3Client = new S3Client({ region, signer: new NoopRequestSigner(), retryStrategy: getRetryStrategy() });
    }
  }

  public static async detectS3Region (bucketName: string) : Promise<string> {
    const response = await axios.head(`https://s3.amazonaws.com/${encodeURIComponent(bucketName)}`, {
      validateStatus: status => status < 400
    });
    const region = response.headers['x-amz-bucket-region'];
    if (region === undefined) {
      throw new Error(`Detect S3 Region failed: ${response.status} - ${response.statusText}`);
    }
    return region;
  }

  public async * listS3Path (path: string, startFrom?: Entry, logger?: winston.Logger) : AsyncGenerator<Entry> {
    const s3Path = path.slice('s3://'.length);
    const bucketName = s3Path.split('/')[0];
    const prefix = s3Path.slice(bucketName.length + 1);
    let token: string | undefined;
    if (startFrom) {
      yield startFrom;
    }
    do {
      const command = new ListObjectsV2Command({
        Bucket: bucketName,
        Prefix: prefix,
        StartAfter: startFrom?.path?.slice('s3://'.length + bucketName.length + 1),
        ContinuationToken: token
      });
      const response: ListObjectsV2CommandOutput = await this.s3Client!.send(command);
      token = response.NextContinuationToken;
      const contents = response.Contents!;
      if (logger) {
        logger.info(`Scanned ${contents.length} entries from ${path}.`, { from: contents[0].Key, to: contents[contents.length - 1].Key });
      } else {
        console.log(`Scanned ${contents.length} entries from ${contents[0].Key} to ${contents[contents.length - 1].Key}`);
      }
      for (const content of response.Contents!) {
        if (content.Key!.endsWith('/')) {
          continue;
        }
        yield {
          path: `s3://${bucketName}/${content.Key!}`,
          size: content.Size!
        };
      }
    } while (token !== undefined);
  }

  private static async * listPath (path: string, lastPath?: string): AsyncGenerator<Entry> {
    for await (const entry of rrdir(path, {
      stats: true, followSymlinks: true, sort: true, startFrom: lastPath
    })) {
      if (entry.err) {
        throw entry.err;
      }
      if (!entry.directory) {
        yield {
          path: entry.path,
          size: entry.stats.size
        };
      }
    }
  }

  private static async isLocalPathAccessible (path: string) : Promise<boolean> {
    try {
      await fs.access(path, constants.R_OK);
    } catch {
      return false;
    }

    return true;
  }

  private async isS3PathAccessible (path: string) : Promise<boolean> {
    const s3Path = path.slice('s3://'.length);
    const bucketName = s3Path.split('/')[0];
    const key = s3Path.slice(bucketName.length + 1);
    const command = new HeadObjectCommand({
      Bucket: bucketName,
      Key: key
    });
    try {
      await this.s3Client!.send(command);
    } catch (error) {
      return false;
    }

    return true;
  }

  private isPathAccessible (path: string) : Promise<boolean> {
    if (path.startsWith('s3://')) {
      return this.isS3PathAccessible(path);
    } else {
      return Scanner.isLocalPathAccessible(path);
    }
  }

  public async * scan (root: string, minSize: number, maxSize: number,
    last: FileInfo | undefined, logger: winston.Logger, skipInaccessibleFiles?: boolean)
    : AsyncGenerator<FileList> {
    let currentList: FileList = [];
    let currentSize = 0;
    let entries;
    if (root.startsWith('s3://')) {
      if (last) {
        entries = this.listS3Path(root, {
          path: last.path,
          size: last.size
        }, logger);
      } else {
        entries = this.listS3Path(root, undefined, logger);
      }
    } else {
      entries = Scanner.listPath(root, last?.path);
    }
    for await (const entry of entries) {
      if (skipInaccessibleFiles && !await this.isPathAccessible(entry.path)) {
        logger.warn(`Skipping inaccessible file ${entry.path}`);
        continue;
      }
      if (last && last.path === entry.path) {
        if (last.end === undefined || last.end === last.size) {
          last = undefined;
          continue;
        } else {
          entry.size = entry.size - last.end;
          entry.offset = last.end;
          last = undefined;
        }
      }
      const newSize = currentSize + entry.size;
      if (newSize <= maxSize) {
        if (!entry.offset) {
          currentList.push({
            size: entry.size,
            path: entry.path
          });
        } else {
          currentList.push({
            size: entry.size + entry.offset,
            path: entry.path,
            start: entry.offset,
            end: entry.size + entry.offset
          });
        }
        currentSize = newSize;
        if (newSize >= minSize) {
          yield currentList;
          currentList = [];
          currentSize = 0;
        }
      } else {
        let remaining = entry.size;
        do {
          let splitSize = minSize - currentSize;
          if (splitSize > remaining) {
            splitSize = remaining;
          }
          if (!entry.offset) {
            currentList.push({
              size: entry.size,
              start: entry.size - remaining,
              end: entry.size - remaining + splitSize,
              path: entry.path
            });
          } else {
            currentList.push({
              size: entry.size + entry.offset,
              start: entry.size - remaining + entry.offset,
              end: entry.size - remaining + splitSize + entry.offset,
              path: entry.path
            });
          }
          currentSize += splitSize;
          remaining -= splitSize;
          if (currentSize >= minSize) {
            yield currentList;
            currentList = [];
            currentSize = 0;
          }
        } while (remaining > 0);
      }
    }
    if (currentList.length > 0) {
      yield currentList;
    }
  }
}
