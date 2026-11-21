import { z } from 'zod';

export type Config = {
  databaseUrl: string;
  coreUrl: string;
  webOrigin: string;
  port: number;
  cookieSecure: boolean;
};

const httpURL = z.url({ protocol: /^https?$/ }).transform((value) => value.replace(/\/+$/, ''));
const envSchema = z.object({
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  ROWAN_CORE_URL: httpURL,
  ROWAN_WEB_ORIGIN: httpURL.refine((value) => new URL(value).origin === value, 'must be a canonical origin'),
  PORT: z.string().regex(/^\d+$/).transform(Number).pipe(z.number().int().min(1).max(65535)).default(8081),
  ROWAN_COOKIE_SECURE: z.enum(['true', 'false']).default('true').transform((value) => value === 'true'),
});

export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const result = envSchema.safeParse(env);
  if (!result.success) {
    const issue = result.error.issues[0];
    throw new Error(`Invalid ${issue?.path.join('.')}: ${issue?.message}`);
  }
  return {
    databaseUrl: result.data.DATABASE_URL,
    coreUrl: result.data.ROWAN_CORE_URL,
    webOrigin: result.data.ROWAN_WEB_ORIGIN,
    port: result.data.PORT,
    cookieSecure: result.data.ROWAN_COOKIE_SECURE,
  };
}
