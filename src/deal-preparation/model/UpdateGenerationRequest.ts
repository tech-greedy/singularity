export default interface UpdateGenerationRequest {
  action?: 'resume' | 'pause' | 'retry' | 'forceRetry',
  outDir?: string,
  tmpDir: string | undefined | null
}
