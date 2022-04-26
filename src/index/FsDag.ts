import { CID } from 'ipfs-core';

export interface Source {
  dataCid: string,
  pieceCid: string,
  selector: number[],
  from?: number,
  to?: number,
}

export interface FileNode {
  sources: Map<string, Source>,
  name: string,
  size: number,
  type: 'file'
}

export interface DirNode {
  sources: Map<string, Source>,
  name: string,
  entries: Map<string, FileNode | DirNode | CID>,
  type: 'dir'
}
