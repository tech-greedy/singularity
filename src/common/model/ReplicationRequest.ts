export default interface ReplicationRequest {
  id: string,
  datasetId: string,
  minReplicas: number,
  criteria: string,
  client: string,
  status: 'active' | 'paused' | 'completed',
}
