export default interface ImportOptions {
  client?: string[],
  path?: string[],
  since: number,
  urlTemplate?: string,
  downloadThreads: number,
  downloadFolder?: string,
  removeImported: boolean,
  interval: number,
  intervalCap: number,
  downloadConcurrency: number,
  importConcurrency: number,
  dryRun: boolean,
  loop: boolean
}
