export interface FileInfo {
  path: string,
  name: string,
  size: number,
  start: number,
  end: number,
}

export type FileList = FileInfo[];

export default interface GenerationRequest {
  id: string
  name: string,
  path: string,
  index: number,
  fileList: FileList
  workerId?: string,
  status: 'active' | 'paused' | 'removed' | 'completed' | 'error',
  errorMessage: string,
  dataCid: string,
  pieceCid: string,
  pieceSize: number
}
