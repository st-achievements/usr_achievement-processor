import { randomInt } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';

import { Injectable } from '@nestjs/common';
import { FirebaseAdminFirestore, Logger, RetryEvent } from '@st-api/firebase';
import dayjs from 'dayjs';

@Injectable()
export class LockService {
  constructor(private readonly firestore: FirebaseAdminFirestore) {}

  private readonly collectionName = 'achievements-processor-lock';
  private readonly logger = Logger.create(this);

  async assert(key: string): Promise<void> {
    const delayMs = randomInt(1, 100);
    await setTimeout(delayMs);
    const docRef = this.firestore.collection(this.collectionName).doc(key);
    const value = await this.firestore.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(docRef);
      if (!snapshot.exists) {
        this.logger.info(`locking for ${key}`);
        transaction.set(docRef, {
          t: dayjs().add(20, 'seconds').toDate(),
        });
        return true;
      }
      return false;
    });
    if (!value) {
      this.logger.info(`key = ${key} already locked`);
      throw new RetryEvent();
    }
  }

  async release(key: string): Promise<void> {
    this.logger.info(`releasing lock for ${key}`);
    const docRef = this.firestore.collection(this.collectionName).doc(key);
    await this.firestore.runTransaction(async (transaction) => {
      transaction.delete(docRef);
    });
    this.logger.info(`released lock for ${key}`);
  }
}
