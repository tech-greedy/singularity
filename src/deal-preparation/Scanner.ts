import { FileInfo, FileList } from '../common/model/InputFileList';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { rrdir } from './rrdir';

export default class Scanner {
  public static async * scan (root: string, minSize: number, maxSize: number, last?: FileInfo): AsyncGenerator<FileList> {
    let currentList: FileList = [];
    let currentSize = 0;
    for await (const entry of rrdir(root, {
      stats: true, followSymlinks: true, sort: true, startFrom: last?.path
    })) {
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
