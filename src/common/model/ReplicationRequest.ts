/**
 * Each record represents one batch of deals
 */
export default interface ReplicationRequest {
  id: string,
  datasetId: string,
  maxReplicas: number, // targeted replica per piece
  criteria: string, // comma separated SP
  client: string, // deal sent from client address
  urlPrefix: string,
  maxPrice: number, // unit in Fil
  maxNumberOfDeals: number, // per SP, unlimited if 0
  isVerfied: boolean,
  duration: number, // in epoch
  isOffline: boolean,
  status: 'active' | 'paused' | 'completed' | 'error',
  errorMessage?: string
}
