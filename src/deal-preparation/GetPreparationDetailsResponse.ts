export interface GenerationRequestSummary {
  id: string,
  index: number,
  status: 'active' | 'paused' | 'completed' | 'error',
  errorMessage: string,
  dataCid: string,
  pieceCid: string,
  pieceSize: number
}

export default interface GetPreparationDetailsResponse {
  id: string,
  name: string,
  path: string,
  minSize: number,
  maxSize: number,
  scanningStatus: 'active' | 'error' | 'completed',
  errorMessage: string,
  generationRequests: GenerationRequestSummary[]
}
