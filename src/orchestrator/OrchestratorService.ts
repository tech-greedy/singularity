import express from 'express'
import config from 'config'

class OrchestratorService {
  public start(): void {
    const enabled = config.get("orchestrator.enabled");
    const bind = config.get<string>("orchestrator.bind");
    const port = config.get<number>("orchestrator.port");
    const app = express();
    if (enabled) {
      app.listen(port, bind);
    }
  }
}

new OrchestratorService().start();
