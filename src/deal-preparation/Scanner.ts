import { FileInfo, FileList } from '../common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { rrdir } from './rrdir';
import { S3Client, ListObjectsV2Command, ListObjectsV2CommandOutput } from '@aws-sdk/client-s3';
import axios from 'axios';
import NoopRequestSigner from './NoopRequestSigner';
import { getRetryStrategy } from '../common/S3RetryStrategy';

interface Entry {
  path: string,
  size: number,
  offset?: number,
}

export default class Scanner {
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

  public static async * listS3Path (path: string, startFrom?: Entry) : AsyncGenerator<Entry> {
    const s3Path = path.slice('s3://'.length);
    const bucketName = s3Path.split('/')[0];
    const prefix = s3Path.slice(bucketName.length + 1);
    const region = await Scanner.detectS3Region(bucketName);
    const client = new S3Client({ region, signer: new NoopRequestSigner(), retryStrategy: getRetryStrategy() });
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
      const response: ListObjectsV2CommandOutput = await client.send(command);
      token = response.NextContinuationToken;
      for (const content of response.Contents!) {
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

  public static async * scan (root: string, minSize: number, maxSize: number, last?: FileInfo): AsyncGenerator<FileList> {
    let currentList: FileList = [];
    let currentSize = 0;
    let entries;
    if (root.startsWith('s3://')) {
      if (last) {
        entries = Scanner.listS3Path(root, {
          path: last.path,
          size: last.size
        });
      } else {
        entries = Scanner.listS3Path(root);
      }
    } else {
      entries = Scanner.listPath(root, last?.path);
    }
    for await (const entry of entries) {
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
