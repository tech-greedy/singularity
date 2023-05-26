import { ConfigInitializer } from "../../src/common/Config";
import Datastore from "../../src/common/Datastore";

export default async (req, res) => {
  await ConfigInitializer.initialize();
  await Datastore.connect();
  const replicationrequests = await Datastore.ReplicationRequestModel.find({});
  res.status(200).json(replicationrequests);
};
