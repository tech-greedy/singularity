import { CID } from 'ipfs-core';

export interface Source {
  dataCid: string,
  pieceCid: string,
  selector?: number[],
  from?: number,
  to?: number,
}

export interface FileNode {
  sourcesMap: Map<string, Source> | null,
  sources: Source[],
  name: string,
  size: number,
  type: 'file'
}

export interface DirNode {
  sourcesMap: Map<string, Source> | null,
  sources: Source[],
  name: string,
  entries: Map<string, FileNode | DirNode | CID>,
  type: 'dir'
}
