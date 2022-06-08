export interface FileInfo {
  // Absolute
  path: string,
  size: number,
  start: number,
  end: number,
}

export interface GeneratedFileInfo {
  // Relative
  path: string,
  dir: boolean,
  size: number,
  start: number,
  end: number,
  selector: number[],
  cid: string
}

export type FileList = FileInfo[];
export type GeneratedFileList = GeneratedFileInfo[];

/**
 * Each record represents one CAR file
 */
export default interface GenerationRequest {
  id: string,
  datasetId: string
  datasetName: string,
  path: string,
  index: number,
  fileList: FileList,
  generatedFileList: GeneratedFileList,
  workerId?: string,
  status: 'active' | 'paused' | 'completed' | 'error',
  errorMessage?: string,
  dataCid?: string,
  carSize?: number,
  pieceCid?: string,
  pieceSize?: number,
  filenameOverride?: string // when the car name is different from cid
}
