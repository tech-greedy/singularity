export interface GetReplicationsResponseItem {
    id: string,
    datasetId: string,
    replica: number,
    criteria: string,
    client: string,
    maxNumberOfDeals: number,
    status: 'active' | 'paused' | 'completed' | 'error',
    errorMessage?: string,
    replicationCompleted: number,
    replicationActive: number,
    replicationPaused: number,
    replicationTotal: number,
    replicationError: number
  }

export type GetReplicationsResponse = GetReplicationsResponseItem[];
