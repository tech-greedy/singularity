export default interface UpdateReplicationRequest {
    cronSchedule?: string,
    cronMaxDeals?: number,
    cronMaxPendingDeals?: number,
    status: 'active' | 'paused'
  }
