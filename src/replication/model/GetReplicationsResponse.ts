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
    csvOutputDir?: string,
    isForced: boolean,
    errorMessage?: string
  }

export type GetReplicationsResponse = GetReplicationsResponseItem[];
