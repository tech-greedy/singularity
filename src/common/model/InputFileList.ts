export interface FileInfo {
  // Absolute
  path: string,
  size: number,
  start?: number,
  end?: number,
}
export type FileList = FileInfo[];
export default interface InputFileList {
  id: string,
  generationId: string,
  index: number,
  fileList: FileList,
}
