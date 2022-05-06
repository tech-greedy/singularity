export default interface ScanningRequest {
  id: string,
  name: string,
  path: string,
  minSize: number,
  maxSize: number,
  workerId?: string,
  status: 'active' | 'completed' | 'error' | 'paused',
  errorMessage?: string
}
