export interface GetReplicationsResponseItem {
    id: string,
    datasetId: string,
    maxReplicas: number,
    criteria: string,
    client: string,
    status: 'active' | 'paused' | 'completed' | 'error',
    errorMessage?: string,
    replicationCompleted: number,
    replicationActive: number,
    replicationPaused: number,
    replicationTotal: number,
    replicationError: number
  }

export type GetReplicationsResponse = GetReplicationsResponseItem[];
