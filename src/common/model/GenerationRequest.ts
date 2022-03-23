import { FileList } from '../../worker/Scanner';

export default interface GenerationRequest {
  datasetName: string,
  datasetPath: string,
  datasetIndex: number,
  fileList: FileList
  workerId?: string,
  completed: boolean,
}
