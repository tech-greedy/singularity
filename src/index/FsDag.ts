import { CID } from 'ipfs-core';

export interface Source {
  from: number,
  to: number,
  cid: string,
}

export interface FileNode {
  sources: Source[],
  name: string,
  size: number,
  type: 'file'
}

export interface DirNode {
  sources: string[],
  name: string,
  entries: Map<string, FileNode | DirNode | CID>,
  type: 'dir'
}
