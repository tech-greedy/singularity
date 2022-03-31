export default interface DealState {
  id: string,
  client: string,
  provider: string,
  dataCid?: string,
  dealId: number,
  expiration?: number,
  state: 'published' | 'active' | 'slashed'
}
