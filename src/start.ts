import config from 'config';
import Datastore from './common/Datastore';
import DealPreparationService from './deal-preparation/DealPreparationService';
import DealPreparationWorker from './worker/DealPreparationWorker';

async function start () {
  await Datastore.init();
  if (config.get('deal_preparation_service.enabled')) {
    new DealPreparationService().start();
  }
  if (config.get('deal_preparation_worker.enabled')) {
    new DealPreparationWorker().start();
  }
}

start();
