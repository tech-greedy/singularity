import path from 'path';
import fs from 'fs-extra';
import DealReplicationWorker from '../replication/DealReplicationWorker';
import Datastore from './Datastore';
import ObjectsToCsv from 'objects-to-csv';

/**
 * Export replication request to CSV
 */
export default class GenerateCsv {
  public static async generate (id: string, outDir: string): Promise<string> {
    let msg = '';
    fs.mkdirpSync(outDir);
    const replicationRequest = await Datastore.ReplicationRequestModel.findById(id);
    if (replicationRequest) {
      const providers = DealReplicationWorker.generateProvidersList(replicationRequest.storageProviders);
      for (let j = 0; j < providers.length; j++) {
        const provider = providers[j];
        const deals = await Datastore.DealStateModel.find({
          replicationRequestId: id,
          provider: provider,
          state: 'proposed'
        });
        let urlPrefix = replicationRequest.urlPrefix;
        if (!urlPrefix.endsWith('/')) {
          urlPrefix += '/';
        }

        if (deals.length > 0) {
          const csvRow = [];
          for (let i = 0; i < deals.length; i++) {
            const deal = deals[i];
            csvRow.push({
              miner_id: deal.provider,
              deal_cid: deal.dealCid,
              filename: `${deal.pieceCid}.car`,
              data_cid: deal.dataCid,
              piece_cid: deal.pieceCid,
              start_epoch: deal.startEpoch,
              full_url: `${urlPrefix}${deal.pieceCid}.car`,
              client: deal.client
            });
          }
          const csv = new ObjectsToCsv(csvRow);
          let fileListFilename = '';
          if (replicationRequest.fileListPath) {
            fileListFilename += '_' + path.parse(replicationRequest.fileListPath).name;
          }
          const filename = path.join(outDir, `${provider}${fileListFilename}_${id}.csv`);
          await csv.toDisk(filename);
          msg += `CSV saved to ${filename}\n`;
        } else {
          msg += `No deal found to export in ${id}\n`;
        }
      }
    } else {
      msg = `Replication request not found ${id}`;
    }
    return msg;
  }
}
