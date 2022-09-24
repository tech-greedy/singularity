import Datastore from '../src/common/Datastore';
import { ConfigInitializer } from '../src/common/Config';

export default class Utils {
  private static initialized = false;
  public static async initDatabase() {
    if (!Utils.initialized) {
      ConfigInitializer.initialize();
      Utils.initialized = true;
      await Datastore.init(true);
      const uri = Datastore['mongoMemoryServer'].getUri();
      await Datastore.connectMongoDb(uri);
      Datastore['setupDataModels']();
    }
  }
}
