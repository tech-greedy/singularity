export interface GetPreparationsResponseItem {
  id: string,
  name: string,
  path: string,
  minSize: number,
  maxSize: number,
  scanningStatus: 'active' | 'error' | 'completed',
  errorMessage?: string,
  generationCompleted: number,
  generationActive: number,
  generationPaused: number,
  generationTotal: number,
  generationError: number
}

export type GetPreparationsResponse = GetPreparationsResponseItem[];
