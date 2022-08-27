export default interface ImportOptions {
  client?: string[],
  path?: string[],
  urlPrefix?: string,
  downloadConcurrency: number,
  downloadFolder?: string,
  removeImported: boolean,
  importInterval: number,
  maxConcurrentImports: number,
  dryRun: boolean,
  loop: boolean
}
