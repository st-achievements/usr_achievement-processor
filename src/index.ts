import {
  AchievementsCoreAdapter,
  REDIS_CREDENTIALS,
} from '@st-achievements/core';
import { StFirebaseApp } from '@st-api/firebase';
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';

import { appHandler } from './app.handler.js';
import { AppModule } from './app.module.js';

dayjs.extend(weekOfYear);

const app = StFirebaseApp.create(AppModule, {
  secrets: [REDIS_CREDENTIALS],
  adapter: new AchievementsCoreAdapter(),
}).addPubSub(appHandler);

export const usr_achievement = {
  processor: {
    events: app.getCloudEventHandlers(),
    http: app.getHttpHandler(),
  },
};
