export default interface DealState {
  datasetId: string,
  client: string,
  provider: string,
  proposalCid: string,
  dataCid: string,
  dealId: number,
  sectorId: number,
  activation: number,
  expiration: number,
  state: 'proposed' | 'failed' | 'published' | 'active' | 'slashed',
  errorMessage: string
}
