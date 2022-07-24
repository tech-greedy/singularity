import { FileInfo, FileList } from '../common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { Entry, rrdir } from './rrdir';
import { S3Client, ListObjectsV2Command, ListObjectsV2CommandOutput } from '@aws-sdk/client-s3';
import { RequestSigner } from '@aws-sdk/types/dist-types/signature';
import { HttpRequest, RequestSigningArguments } from '@aws-sdk/types';
import axios from 'axios';

class NoopRequestSigner implements RequestSigner {
  public sign (requestToSign: HttpRequest, _options?: RequestSigningArguments): Promise<HttpRequest> {
    return Promise.resolve(requestToSign);
  }
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

  public static async * listS3Path (path: string, lastPath?: string) : AsyncGenerator<Entry<string>> {
    const bucketName = path.split('/')[0];
    const region = await Scanner.detectS3Region(bucketName);
    const client = new S3Client({ region, signer: new NoopRequestSigner() });
    const command = new ListObjectsV2Command({
      Bucket: path,
      StartAfter: lastPath
    });
    const response: ListObjectsV2CommandOutput = await client.send(command);
    for (const content of response.Contents!) {
      yield {
        path: content.Key!,
        directory: false,
        stats: {
          size: content.Size!
        }
      };
    }
  }

  private static listPath (path: string, lastPath?: string): AsyncGenerator<Entry<string>> {
    return rrdir(path, {
      stats: true, followSymlinks: true, sort: true, startFrom: lastPath
    });
  }

  public static async * scan (root: string, minSize: number, maxSize: number, last?: FileInfo): AsyncGenerator<FileList> {
    let currentList: FileList = [];
    let currentSize = 0;
    let entries;
    if (root.startsWith('s3://')) {
      entries = Scanner.listS3Path(root, last?.path);
    } else {
      entries = Scanner.listPath(root, last?.path);
    }
    for await (const entry of entries) {
      if (entry.directory) {
        continue;
      }
      if (entry.err) {
        throw entry.err;
      }
      if (last && last.path === entry.path) {
        if (last.end === undefined || last.end === last.size) {
          last = undefined;
          continue;
        } else {
          entry.stats!.size = entry.stats!.size - last.end;
          entry.offset = last.end;
          last = undefined;
        }
      }
      const newSize = currentSize + entry.stats!.size;
      if (newSize <= maxSize) {
        if (!entry.offset) {
          currentList.push({
            size: entry.stats!.size,
            path: entry.path
          });
        } else {
          currentList.push({
            size: entry.stats!.size + entry.offset,
            path: entry.path,
            start: entry.offset,
            end: entry.stats!.size + entry.offset
          });
        }
        currentSize = newSize;
        if (newSize >= minSize) {
          yield currentList;
          currentList = [];
          currentSize = 0;
        }
      } else {
        let remaining = entry.stats!.size;
        do {
          let splitSize = minSize - currentSize;
          if (splitSize > remaining) {
            splitSize = remaining;
          }
          if (!entry.offset) {
            currentList.push({
              size: entry.stats!.size,
              start: entry.stats!.size - remaining,
              end: entry.stats!.size - remaining + splitSize,
              path: entry.path
            });
          } else {
            currentList.push({
              size: entry.stats!.size + entry.offset,
              start: entry.stats!.size - remaining + entry.offset,
              end: entry.stats!.size - remaining + splitSize + entry.offset,
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
