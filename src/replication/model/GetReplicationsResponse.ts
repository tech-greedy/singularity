export interface GetReplicationsResponseItem {
    id: string,
    datasetId: string,
    replica: number,
    storageProviders: string,
    client: string,
    maxNumberOfDeals: number,
    status: 'active' | 'paused' | 'completed' | 'error',
    cronSchedule?: string,
    cronMaxDeals?: number,
    cronMaxPendingDeals?: number,
    fileListPath?: string,
    notes?: string,
    dealsTotal?: number,
    dealsProposed?: number,
    dealsPublished?: number,
    dealsActive?: number,
    dealsProposalExpired?: number,
    dealsExpired?: number,
    dealsSlashed?: number,
    dealsError?: number
  }

export type GetReplicationsResponse = GetReplicationsResponseItem[];
