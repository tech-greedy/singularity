export default interface ScanningRequest {
  id: string,
  name: string,
  path: string,
  outDir: string,
  minSize: number,
  maxSize: number,
  workerId?: string,
  status: 'active' | 'completed' | 'error' | 'paused',
  errorMessage?: string
}
