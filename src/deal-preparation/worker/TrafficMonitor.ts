export default class TrafficMonitor {
  public downloaded;
  private downloadStartTimestamp;

  public constructor (private intervalMs: number) {
    this.downloaded = 0;
    this.downloadStartTimestamp = 0;
  }

  public countNewChunk (chunk: any) {
    const ts = Date.now();
    if (ts >= this.downloadStartTimestamp + this.intervalMs) {
      this.downloadStartTimestamp = ts - ts % this.intervalMs;
      this.downloaded = 0;
    }
    this.downloaded += chunk.length;
  }
}
