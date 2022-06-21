export default interface CreatePreparationRequest {
  name : string,
  path : string,
  dealSize : string,
  outDir: string,
  minRatio?: number,
  maxRatio?: number,
  tmpDir?: string
}
