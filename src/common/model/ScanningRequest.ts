export default interface ScanningRequest {
  id: string,
  datasetName: string,
  datasetPath: string,
  minSize: string,
  maxSize: string,
  workerId?: string,
  completed: boolean,
}
