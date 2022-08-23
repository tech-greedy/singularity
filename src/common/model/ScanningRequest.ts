/**
 * Each record represents one dataset (usually within one folder, can have subfolders.)
 */
export default interface ScanningRequest {
  id: string,
  name: string,
  path: string,
  outDir: string,
  minSize: number,
  maxSize: number,
  workerId?: string,
  status: 'active' | 'completed' | 'error' | 'paused',
  errorMessage?: string,
  tmpDir?: string,
  scanned: number
}
