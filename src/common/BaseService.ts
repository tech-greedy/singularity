import winston from 'winston';
import Logger, { Category } from './Logger';
import config from './Config';
import { randomUUID } from 'crypto';
import pidusage from 'pidusage';
import Datastore from './Datastore';
import { AbortSignal } from './AbortSignal';
import { GenerationProcessor } from '../deal-preparation/worker/GenerationProcessor';

export default abstract class BaseService {
  public readonly workerId: string;
  public readonly type: string;
  public logger: winston.Logger;
  protected enabled: boolean;

  protected constructor (category: Category) {
    this.workerId = randomUUID();
    this.type = category.toString();
    this.logger = Logger.getLogger(category);
    this.enabled = config.get(`${category}.enabled`);
  }

  public abstract start (): void;

  private async startUpdateUsage (abortSignal?: AbortSignal): Promise<void> {
    if (abortSignal && await abortSignal()) {
      return;
    }
    await this.updateUsage();
    setTimeout(async () => this.startUpdateUsage(abortSignal), 5000);
  }

  public async initialize (abortSignal?: AbortSignal): Promise<void> {
    await Datastore.HealthCheckModel.create({
      workerId: this.workerId,
      type: this.type,
      state: 'idle',
      pid: process.pid,
      downloadSpeed: 0
    });
    this.startUpdateUsage(abortSignal);
  }

  private async updateUsage (): Promise<void> {
    const pid = process.pid;
    const childPid = GenerationProcessor.childProcessPid;
    if (childPid) {
      const usage = await pidusage([pid, childPid]);
      const cpuUsage = usage[pid]?.cpu;
      const memoryUsage = usage[pid]?.memory;
      const childCpuUsage = usage[childPid]?.cpu;
      const childMemoryUsage = usage[childPid]?.memory;
      await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, {
        $set: {
          pid,
          cpuUsage,
          memoryUsage,
          childPid,
          childCpuUsage,
          childMemoryUsage
        }
      });
    } else {
      const usage = await pidusage([pid]);
      const cpuUsage = usage[pid]?.cpu;
      const memoryUsage = usage[pid]?.memory;
      await Datastore.HealthCheckModel.findOneAndUpdate({ workerId: this.workerId }, {
        $set: {
          pid,
          cpuUsage,
          memoryUsage
        },
        $unset: {
          childPid: '',
          childCpuUsage: '',
          childMemoryUsage: ''
        }
      });
    }
  }
}
