/**
 * Represents one deal sent out by this system
 */
export default interface DealSelfServicePolicy {
  id: string,
  client: string,
  provider: string,
  datasetName: string,
  minStartDays: number,
  maxStartDays: number,
  verified: boolean,
  price: number,
  minDurationDays: number,
  maxDurationDays: number,
}
