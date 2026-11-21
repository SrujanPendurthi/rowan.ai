// ponytail: per-process limiter; move to PG if more than one BFF instance runs.
export class LoginLimiter {
  private readonly failures = new Map<string, number[]>();

  private readonly maxEntries: number;

  constructor(maxEntries = 10_000) {
    this.maxEntries = maxEntries;
  }

  private recent(email: string, now: Date): number[] {
    const recent = (this.failures.get(email) ?? []).filter((time) => time > now.getTime() - 15 * 60 * 1000);
    if (recent.length) this.failures.set(email, recent);
    else this.failures.delete(email);
    return recent;
  }

  check(email: string, now: Date): number {
    const recent = this.recent(email, now);
    const oldest = recent[0];
    return recent.length >= 5 && oldest !== undefined
      ? Math.ceil((oldest + 15 * 60 * 1000 - now.getTime()) / 1000) : 0;
  }

  fail(email: string, now: Date): void {
    const recent = this.recent(email, now);
    recent.push(now.getTime());
    this.failures.set(email, recent);
    if (this.failures.size > this.maxEntries) {
      const cutoff = now.getTime() - 15 * 60 * 1000;
      for (const [key, failures] of this.failures) {
        const newest = failures.at(-1);
        if (newest === undefined || newest < cutoff) this.failures.delete(key);
      }
    }
  }

  reset(email: string): void {
    this.failures.delete(email);
  }
}
