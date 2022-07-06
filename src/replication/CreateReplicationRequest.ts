export default interface CreateReplicationRequest {
    datasetId: string,
    replica: number,
    storageProviders: string,
    client: string,
    urlPrefix: string,
    maxPrice: number,
    maxNumberOfDeals: number,
    isVerfied: string,
    startDelay: number,
    duration: number,
    isOffline: string,
    cronSchedule?: string,
    cronMaxDeals?: number
  }
