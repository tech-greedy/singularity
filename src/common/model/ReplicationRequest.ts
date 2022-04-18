/**
 * Each record represents one batch of deals
 */
export default interface ReplicationRequest {
  id: string,
  datasetId: string,
  minReplicas: number, // not used now
  criteria: string, // comma separated SP
  client: string, // deal sent from client address
  urlPrefix: string,
  maxPrice: number, // unit in Fil
  maxNumberOfDeals: number, // per SP
  isVerfied: boolean,
  duration: number, // in days, for example 365, 500
  isOffline: boolean,
  status: 'active' | 'paused' | 'completed' | 'error',
  errorMessage?: string
}
