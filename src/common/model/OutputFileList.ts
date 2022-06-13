export interface GeneratedFileInfo {
  // Relative
  path: string,
  dir: boolean,
  size?: number,
  start?: number,
  end?: number,
  selector: number[],
  cid: string
}

export type GeneratedFileList = GeneratedFileInfo[];
export default interface OutputFileList {
  id: string,
  generationId: string,
  index: number,
  generatedFileList: GeneratedFileList,
}
