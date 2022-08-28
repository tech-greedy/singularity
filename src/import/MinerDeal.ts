export interface Cid {
  '/': string;
}

export interface Proposal {
  PieceCID?: Cid,
  PieceSize?: number,
  VerifiedDeal?: boolean,
  Client: string,
  Provider: string,
  Label?: string,
  StartEpoch?: number,
  EndEpoch?: number,
  StoragePricePerEpoch?: string,
  ProviderCollateral?: string,
  ClientCollateral?: string
}

export default interface MinerDeal {
  Proposal: Proposal,
  ProposalCid: Cid,
  State: number,
  Refs: {
    Root: Cid,
    PieceCid: Cid,
    PieceSize: number
  },
  CreationTime: string
}
