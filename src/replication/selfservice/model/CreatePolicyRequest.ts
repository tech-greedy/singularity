export default interface CreatePolicyRequest {
  client: string,
  provider: string,
  dataset: string,
  minStartDays: number,
  maxStartDays: number,
  verified: boolean,
  price: number,
  minDurationDays: number,
  maxDurationDays: number,
}
