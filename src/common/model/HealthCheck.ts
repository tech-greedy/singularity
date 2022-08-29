export default interface HealthCheck {
  workerId: string,
  type: string,
  downloadSpeed: number,
  state: 'idle' | 'scanning' | 'generation_start' | 'generation_saving_output' |
    'generation_parsing_output' | 'generation_generating_car_and_commp' | 'generation_moving_to_tmpdir'
  updatedAt: Date,
  pid: number,
  cpuUsage?: number,
  memoryUsage?: number,
  childPid?: number,
  childCpuUsage?: number,
  childMemoryUsage?: number,
}
