import { randomInt } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';

import { Injectable } from '@nestjs/common';
import { FirebaseAdminFirestore, RetryEvent } from '@st-api/firebase';
import dayjs from 'dayjs';

@Injectable()
export class LockService {
  constructor(private readonly firestore: FirebaseAdminFirestore) {}

  private readonly collectionName = 'achievements-processor-lock';

  async assert(key: string): Promise<void> {
    const delayMs = randomInt(1, 100);
    await setTimeout(delayMs);
    const docRef = this.firestore.collection(this.collectionName).doc(key);
    const value = await this.firestore.runTransaction(async (transaction) => {
      const snapshot = await transaction.get(docRef);
      if (!snapshot.exists) {
        transaction.set(docRef, {
          t: dayjs().add(20, 'seconds').toDate(),
        });
        return true;
      }
      return false;
    });
    if (!value) {
      throw new RetryEvent();
    }
  }

  async release(key: string): Promise<void> {
    const docRef = this.firestore.collection(this.collectionName).doc(key);
    await this.firestore.runTransaction(async (transaction) => {
      transaction.delete(docRef);
    });
  }
}
