/**
 * Each record represents one batch of deals
 */
export default interface ReplicationRequest {
  id: string,
  workerId?: string,
  datasetId: string,
  maxReplicas: number, // targeted replica per piece
  storageProviders: string, // comma separated SP
  client: string, // deal sent from client address
  urlPrefix: string,
  maxPrice: number, // unit in Fil
  maxNumberOfDeals: number, // per SP, unlimited if 0
  isVerfied: boolean,
  startDelay: number, // in epoch
  duration: number, // in epoch
  isOffline: boolean,
  status: 'active' | 'paused' | 'completed' | 'error',
  cronSchedule?: string, // if specified, each cron will trigger sending the next maxNumberOfDeals
  cronMaxDeals?: number, // per SP total with cron considered
  cronMaxPendingDeals?: number, // per SP pending total with cron considered
  fileListPath?: string, // limit to replicate only from the list in a txt file
  notes?: string, // any notes or tag want to store along the replication request, for tracking purpose
  csvOutputDir?: string, // folder to print CSV to, undefined to skip the CSV
  isForced: boolean,
  errorMessage?: string
}
