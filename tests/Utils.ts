import Datastore from '../src/common/Datastore';
import { ConfigInitializer, config } from '../src/common/Config';

export default class Utils {
  private static initialized = false;
  public static async initDatabase() {
    if (!Utils.initialized) {
      await ConfigInitializer.initialize(true);
      Utils.initialized = true;
      await Datastore.init(true);
      const uri = Datastore['mongoMemoryServer'].getUri();
      await Datastore.connectMongoDb(uri);
      Datastore['setupDataModels']();
      config.set('metrics.enabled', false);
    }
  }
}
