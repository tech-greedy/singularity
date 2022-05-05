export interface FileInfo {
  path: string,
  size: number,
  selector: number[],
  start: number,
  end: number,
}

export type FileList = FileInfo[];

export default interface GenerationRequest {
  id: string,
  datasetId: string
  datasetName: string,
  path: string,
  index: number,
  fileList: FileList
  workerId?: string,
  status: 'active' | 'paused' | 'completed' | 'error',
  errorMessage?: string,
  dataCid?: string,
  carSize?: number,
  pieceCid?: string,
  pieceSize?: number
}
