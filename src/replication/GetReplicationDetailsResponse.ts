export default interface GetReplicationDetailsResponse {
    id: string,
    datasetId: string,
    criteria: string,
    client: string,
    replica: number,
    urlPrefix: string,
    maxPrice: number,
    maxNumberOfDeals: number,
    isVerfied: string,
    duration: number,
    isOffline: string,
    status: 'active' | 'paused' | 'completed' | 'error',
    cronSchedule?: string,
    cronMaxDeals?: number
  }
