import Datastore from '../src/common/Datastore';

export default class Utils {
  private static initialized = false;
  public static async initDatabase() {
    if (!Utils.initialized) {
      await Datastore['setupLocalMongoDb']('0.0.0.0', 26999);
      await Datastore['connectMongoDb']('mongodb://127.0.0.1:26999');
      Datastore['setupDataModels']();
    }
    Utils.initialized = true;
  }
}
