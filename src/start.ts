import config from 'config';
import Datastore from './common/Datastore';
import OrchestratorService from './orchestrator/OrchestratorService';
import DealPreparationWorker from './worker/DealPreparationWorker';

async function start () {
  await Datastore.init();
  if (config.get('orchestrator.enabled')) {
    new OrchestratorService().start();
  }
  if (config.get('deal_preparation_worker.enabled')) {
    new DealPreparationWorker().start();
  }
}

start();
