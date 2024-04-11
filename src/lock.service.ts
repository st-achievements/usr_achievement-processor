import { setTimeout } from 'node:timers/promises';

import { Injectable } from '@nestjs/common';
import { Logger, RetryEvent } from '@st-api/firebase';
import { Redis } from 'ioredis';

@Injectable()
export class LockService {
  constructor(private readonly redis: Redis) {}

  private readonly logger = Logger.create(this);
  private delayMs = 0;

  private async wait(): Promise<void> {
    if (this.delayMs >= 100) {
      this.delayMs = 0;
    }
    await setTimeout(this.delayMs++);
  }

  async assert(key: string, context: string): Promise<void> {
    this.logger.info(`[${context}] key = ${key} checking lock`);
    await this.wait();
    const result = await this.redis.set(key, 'locked', 'EX', 10, 'NX');
    if (result !== 'OK') {
      this.logger.info(`[${context}] key = ${key} already locked`);
      throw new RetryEvent();
    }
    this.logger.info(`[${context}] key = ${key} locked`);
  }

  async release(key: string, context: string): Promise<void> {
    this.logger.info(`[${context}] releasing lock for ${key}`);
    await this.redis.del(key);
    this.logger.info(`[${context}] released lock for ${key}`);
  }
}
