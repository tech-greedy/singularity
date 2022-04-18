export default interface CreateReplicationRequest {
    datasetId: string,
    replica: number,
    criteria: string,
    client: string,
    urlPrefix: string,
    maxPrice: number,
    maxNumberOfDeals: number,
    isVerfied: string,
    duration: number,
    isOffline: string,
  }
