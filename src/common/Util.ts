export function sleep (ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function getNextPowerOfTwo (n: number): number {
  return Math.pow(2, Math.ceil(Math.log2(n)));
}
