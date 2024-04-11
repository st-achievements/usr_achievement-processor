import { FactoryProvider } from '@nestjs/common';
import { formatZodErrorString } from '@st-api/core';
import { defineSecret } from 'firebase-functions/params';
import { Redis } from 'ioredis';
import { z } from 'zod';

import { INVALID_REDIS_CREDENTIALS } from './exceptions.js';

export const REDIS_CREDENTIALS: ReturnType<typeof defineSecret> =
  defineSecret('REDIS_CREDENTIALS');

const RedisCredentialsSchema = z
  .string()
  .trim()
  .min(1)
  .transform((value) => JSON.parse(value))
  .pipe(
    z.object({
      host: z.string().trim().min(1),
      password: z.string().trim().min(1),
      port: z.number().safe().int().positive(),
    }),
  );

export const RedisProvider: FactoryProvider = {
  provide: Redis,
  useFactory: () => {
    const result = RedisCredentialsSchema.safeParse(REDIS_CREDENTIALS.value());
    if (!result.success) {
      throw INVALID_REDIS_CREDENTIALS(formatZodErrorString(result.error));
    }
    const credentials = result.data;
    return new Redis({
      password: credentials.password,
      host: credentials.host,
      port: credentials.port,
      commandTimeout: 1000,
      lazyConnect: true,
    });
  },
};
