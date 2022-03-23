import glob from 'fast-glob';
import path from 'path';
import { FileList } from '../common/model/GenerationRequest';

export default class Scanner {
  public static async scan (root: string, minSize: number, maxSize: number): Promise<FileList[]> {
    // Get all files
    let entries = await glob(path.join(root, '**', '*'), {
      onlyFiles: true,
      stats: true
    });
    // By default it is unordered so sort by path name
    entries = entries.sort((a, b) => a.path.localeCompare(b.path));
    // Iterating through entries
    const result: FileList[] = [];
    let currentList: FileList = [];
    let currentSize = 0;
    for (const entry of entries) {
      const newSize = currentSize + entry.stats!.size;
      if (newSize >= minSize && newSize <= maxSize) {
        currentList.push({
          size: entry.stats!.size,
          start: 0,
          end: 0,
          path: entry.path,
          name: entry.name
        });
        result.push(currentList);
        currentList = [];
        currentSize = 0;
      } else if (newSize > maxSize) {
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
            name: entry.name
          });
          currentSize += splitSize;
          remaining -= splitSize;
          if (currentSize >= minSize) {
            result.push(currentList);
            currentList = [];
            currentSize = 0;
          }
        } while (remaining > 0);
      }
    }
    if (currentList.length > 0) {
      result.push(currentList);
    }

    return result;
  }
}
