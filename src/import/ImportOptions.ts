export default interface ImportOptions {
  client?: string[],
  path?: string[],
  urlTemplate?: string,
  downloadThreads: number,
  downloadFolder?: string,
  removeImported: boolean,
  interval: number,
  concurrency: number,
  importConcurrency: number,
  dryRun: boolean,
  loop: boolean
}
