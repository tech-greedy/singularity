export default interface GenerationRequest {
  id: string,
  datasetId: string
  datasetName: string,
  path: string,
  outDir: string,
  index: number,
  workerId?: string,
  status: 'active' | 'paused' | 'completed' | 'error' | 'created' | 'dag',
  errorMessage?: string,
  dataCid?: string,
  carSize?: number,
  pieceCid?: string,
  pieceSize?: number,
  filenameOverride?: string, // when the car name is different from cid
  tmpDir?: string,
  skipInaccessibleFiles?: boolean,
  updatedAt?: Date,
}
