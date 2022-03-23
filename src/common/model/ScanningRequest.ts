export default interface ScanningRequest {
  id: string,
  name: string,
  path: string,
  minSize: number,
  maxSize: number,
  workerId?: string,
  status: 'active' | 'paused' | 'removed' | 'completed' | 'error',
  errorMessage: string
}
