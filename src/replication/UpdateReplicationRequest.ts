export default interface UpdateReplicationRequest {
    cronSchedule?: string,
    cronMaxDeals?: number,
    status: 'active' | 'paused'
  }
