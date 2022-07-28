export interface GetPreparationsResponseItem {
  id: string,
  name: string,
  path: string,
  minSize: number,
  maxSize: number,
  outDir: string,
  scanningStatus: 'active' | 'error' | 'completed' | 'paused',
  scanned: number,
  errorMessage?: string,
  generationCompleted: number,
  generationActive: number,
  generationPaused: number,
  generationTotal: number,
  generationError: number
}

export type GetPreparationsResponse = GetPreparationsResponseItem[];
