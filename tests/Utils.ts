import Datastore from '../src/common/Datastore';

export default class Utils {
  private static initialized = false;
  public static async initDatabase() {
    if (!Utils.initialized) {
      Utils.initialized = true;
      await Datastore.init(true);
    }
  }
}
