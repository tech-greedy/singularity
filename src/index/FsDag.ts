import {CID} from 'ipfs-core';

/**
 * {
 *   "sources": [
 *     {
 *       index: 0,
 *       array: [{
 *         index: 0,
 *         array: ["a", "b"]
 *       }]
 *     },
 *     {
 *       index: 1,
 *       array: ["c"]
 *     }
 *   ]
 * }
 */
export interface LayeredArray<T> {
  index: number,
  // eslint-disable-next-line no-use-before-define
  array: DynamicArray<T> | CID
}

export type DynamicArray<T> = T[] | LayeredArray<T>[];

/**
 * {
 *   "sources": [
 *     {
 *       index: 0,
 *       map: [
 *         {
 *           index: 0,
 *           map: { a: "a", b: "b"}
 *         }
 *       ]
 *     },
 *     {
 *       index: 1,
 *       map: { c: "c" }
 *     }
 *   ]
 * }
 */
export interface LayeredMap<T> {
  from: string,
  to: string,
  // eslint-disable-next-line no-use-before-define
  map: DynamicMap<T> | CID
}

export type DynamicMap<T> = Map<string, T> | LayeredMap<T>[];

export interface Source {
  from: number,
  to: number,
  cid: string,
}

export interface FileNode {
  sources?: Source[],
  realSources?: DynamicArray<Source> | CID,
  name: string,
  size: number,
  type: 'file'
}

export interface DirNode {
  sources?: string[],
  realSources?: DynamicArray<string> | CID,
  name: string,
  entries?: Map<string, FileNode | DirNode | CID>,
  realEntries?: DynamicMap<FileNode | DirNode | CID> | CID,
  type: 'dir'
}

export function DynamizeArray<T> (array: T[], maxLink: number): DynamicArray<T> {
  let partitionSize = 1;
  while (array.length > partitionSize * maxLink) {
    partitionSize *= maxLink;
  }
  if (partitionSize === 1) {
    return array;
  }

  const result: LayeredArray<T>[] = [];
  for (let i = 0; i < array.length; i += partitionSize) {
    result.push({
      index: i,
      array: DynamizeArray(array.slice(i, i + partitionSize), maxLink)
    });
  }

  return result;
}

export function DynamizeMap<T> (map: Map<string, T>, maxLink: number): DynamicMap<T> {
  if (map.size <= maxLink) {
    return map;
  }

  let partitionSize = 1;
  while (map.size > partitionSize * maxLink) {
    partitionSize *= maxLink;
  }

  const result: LayeredMap<T>[] = [];
  const mapEntries = Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  for (let i = 0; i < mapEntries.length; i += partitionSize) {
    const slice = mapEntries.slice(i, i + partitionSize);
    result.push({
      from: slice[0][0],
      to: slice[slice.length - 1][0],
      map: DynamizeMap(new Map(slice), maxLink)
    });
  }

  return result;
}
