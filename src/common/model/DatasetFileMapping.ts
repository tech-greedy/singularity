export default interface DatasetFileMapping {
  datasetId: string,
  datasetName: string,
  index: number,
  filePath: string,
  rootCid: string,
  selector: number[]
}
