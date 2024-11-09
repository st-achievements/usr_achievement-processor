import { AchievementsCoreAdapter } from '@st-achievements/core';
import { StFirebaseApp } from '@st-api/firebase';
import dayjs from 'dayjs';
import weekOfYear from 'dayjs/plugin/weekOfYear.js';

import { AppHandler, appHandler } from './app.handler.js';
import { PlatinumService } from './platinum.service.js';
import { LockService } from './lock.service.js';

dayjs.extend(weekOfYear);

const app = StFirebaseApp.create({
  adapter: new AchievementsCoreAdapter({
    authentication: false,
    throttling: false,
  }),
  controllers: [],
  providers: [AppHandler, PlatinumService, LockService],
}).addPubSub(appHandler);

export const usr_achievement = {
  processor: {
    events: app.getCloudEventHandlers(),
    http: app.getHttpHandler(),
  },
};
