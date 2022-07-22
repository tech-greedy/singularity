export default interface GetReplicationDetailsResponse {
    id: string,
    datasetId: string,
    storageProviders: string,
    client: string,
    replica: number,
    urlPrefix: string,
    maxPrice: number,
    maxNumberOfDeals: number,
    isVerfied: string,
    startDelay: number,
    duration: number,
    isOffline: string,
    status: 'active' | 'paused' | 'completed' | 'error',
    cronSchedule?: string,
    cronMaxDeals?: number,
    cronMaxPendingDeals?: number
  }
