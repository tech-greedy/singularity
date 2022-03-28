import rrdir from 'rrdir';
import path from 'path';
import { FileList } from '../common/model/GenerationRequest';

export default class Scanner {
  public static async * scan (root: string, minSize: number, maxSize: number): AsyncGenerator<FileList> {
    let currentList: FileList = [];
    let currentSize = 0;
    for await (const entry of rrdir(root, {
      stats: true, followSymlinks: true
    })) {
      if (entry.directory) {
        continue;
      }
      if (entry.err) {
        throw entry.err;
      }
      const newSize = currentSize + entry.stats!.size;
      if (newSize <= maxSize) {
        currentList.push({
          size: entry.stats!.size,
          start: 0,
          end: 0,
          path: entry.path,
          name: path.basename(entry.path)
        });
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
          currentList.push({
            size: entry.stats!.size,
            start: entry.stats!.size - remaining,
            end: entry.stats!.size - remaining + splitSize,
            path: entry.path,
            name: path.basename(entry.path)
          });
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
