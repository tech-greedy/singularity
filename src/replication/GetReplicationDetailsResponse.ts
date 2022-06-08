export default interface GetReplicationDetailsResponse {
    id: string,
    datasetId: string,
    maxReplicas: number,
    criteria: string,
    client: string,
    status: 'active' | 'paused' | 'completed' | 'error',
  }
