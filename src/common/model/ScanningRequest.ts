export default interface ScanningRequest {
  id: string,
  datasetName: string,
  datasetPath: string,
  minSize: number,
  maxSize: number,
  workerId?: string,
  completed: boolean,
}
