export default interface GetReplicationDetailsResponse {
    id: string,
    datasetId: string,
    minReplicas: number,
    criteria: string,
    client: string,
    status: 'active' | 'paused' | 'completed' | 'error',
  }
