/**
 * Represents one deal sent out by this system
 */
export default interface DealState {
  id: string,
  client: string,
  provider: string,
  dealCid: string,
  dataCid: string,
  pieceCid: string,
  pieceSize: number,
  startEpoch: number,
  expiration: number,
  duration: number,
  price: number, // unit is Fil
  verified: boolean,
  state: 'proposed' | 'published' | 'active' | 'proposal_expired' | 'expired' | 'slashed' | 'error',
  replicationRequestId: string,
  datasetId: string,
  dealId?: number,
  errorMessage?: string// any useful info returned by boost / lotus,
  updatedAt: Date,
  dataCidBeforeCorrection?: string // backup information on dataCid correction
}
